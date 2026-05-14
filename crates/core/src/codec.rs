// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Python-aware extension codecs.
//!
//! [`PythonLogicalCodec`] wraps a user-supplied (or default)
//! [`LogicalExtensionCodec`] and adds in-band encoding of Python-defined
//! scalar UDFs via cloudpickle. [`PythonPhysicalCodec`] is the symmetric
//! wrapper around [`PhysicalExtensionCodec`]; both reuse the same payload
//! framing so a Python `ScalarUDF` round-trips identically through either
//! codec layer.
//!
//! ## Wire-format magic prefix registry
//!
//! Each Python-inline payload begins with an 8-byte magic prefix so the
//! decoder can distinguish it from arbitrary `fun_definition` bytes (e.g.
//! produced by a user FFI codec or left empty by the default codec).
//!
//! | Layer + kind                  | Magic prefix | Owner          |
//! | ----------------------------- | ------------ | -------------- |
//! | `PythonLogicalCodec` scalar   | `DFPYUDF1`   | in use         |
//! | `PythonLogicalCodec` agg      | `DFPYUDA1`   | reserved       |
//! | `PythonLogicalCodec` window   | `DFPYUDW1`   | reserved       |
//! | `PythonPhysicalCodec` scalar  | `DFPYUDF1`   | in use (shared with logical) |
//! | `PythonPhysicalCodec` agg     | `DFPYUDA1`   | reserved       |
//! | `PythonPhysicalCodec` window  | `DFPYUDW1`   | reserved       |
//! | `PythonPhysicalCodec` expr    | `DFPYPE1`    | reserved       |
//! | User FFI extension codec      | user-chosen  | downstream     |
//! | Default codec                 | (none)       | upstream       |
//!
//! Dispatch precedence inside the Python codecs: **Python-inline payload
//! (magic prefix match) → `inner` codec → caller's registry fallback.**
//! When a payload does not match a Python magic prefix the call delegates
//! to `inner`, which is typically `DefaultLogicalExtensionCodec` /
//! `DefaultPhysicalExtensionCodec` but may be a user-supplied FFI codec
//! installed via `SessionContext.with_logical_extension_codec(...)` /
//! `with_physical_extension_codec(...)`.
//!
//! User FFI codecs should pick non-colliding prefixes (recommend a `DF`
//! namespace plus a crate-specific suffix).

use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use arrow::pyarrow::ToPyArrow;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::pyarrow::FromPyArrow;
use datafusion::common::{Result, TableReference};
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan, ScalarUDF, ScalarUDFImpl};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::{DefaultLogicalExtensionCodec, LogicalExtensionCodec};
use datafusion_proto::physical_plan::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};
use pyo3::BoundObject;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

use crate::udf::PythonFunctionScalarUDF;

/// Magic prefix for an inlined Python scalar UDF payload. Shared between
/// logical and physical codec layers — the cloudpickled tuple shape is
/// identical, so a `ScalarUDF` reaching either codec encodes the same way.
pub(crate) const PY_SCALAR_UDF_MAGIC: &[u8] = b"DFPYUDF1";

/// `LogicalExtensionCodec` that serializes Python scalar UDFs inline and
/// delegates every other call to a composable `inner` codec. See module
/// docs for the dispatch precedence.
#[derive(Debug)]
pub struct PythonLogicalCodec {
    inner: Arc<dyn LogicalExtensionCodec>,
}

impl PythonLogicalCodec {
    pub fn new(inner: Arc<dyn LogicalExtensionCodec>) -> Self {
        Self { inner }
    }

    /// Convenience constructor wrapping `DefaultLogicalExtensionCodec`.
    pub fn with_default_inner() -> Self {
        Self::new(Arc::new(DefaultLogicalExtensionCodec {}))
    }

    pub fn inner(&self) -> &Arc<dyn LogicalExtensionCodec> {
        &self.inner
    }
}

impl Default for PythonLogicalCodec {
    fn default() -> Self {
        Self::with_default_inner()
    }
}

impl LogicalExtensionCodec for PythonLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.inner.try_encode_table_provider(table_ref, node, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        if try_encode_python_scalar_udf(node, buf)? {
            return Ok(());
        }
        self.inner.try_encode_udf(node, buf)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        match try_decode_python_scalar_udf(buf)? {
            Some(udf) => Ok(udf),
            None => self.inner.try_decode_udf(name, buf),
        }
    }
}

/// `PhysicalExtensionCodec` mirror of [`PythonLogicalCodec`]. Scalar-UDF
/// payloads use the shared `DFPYUDF1` framing so an `ExecutionPlan` or
/// `PhysicalExpr` that references a Python `ScalarUDF` round-trips
/// regardless of which codec layer encoded it.
///
/// Encoding for Python-defined `ExecutionPlan` impls and `PhysicalExpr`
/// impls (the `DFPYPE*` namespace) is reserved for a future change — no
/// concrete Python-side physical extension type exists today, so all
/// non-UDF calls delegate to `inner`.
#[derive(Debug)]
pub struct PythonPhysicalCodec {
    inner: Arc<dyn PhysicalExtensionCodec>,
}

impl PythonPhysicalCodec {
    pub fn new(inner: Arc<dyn PhysicalExtensionCodec>) -> Self {
        Self { inner }
    }

    pub fn with_default_inner() -> Self {
        Self::new(Arc::new(DefaultPhysicalExtensionCodec {}))
    }

    pub fn inner(&self) -> &Arc<dyn PhysicalExtensionCodec> {
        &self.inner
    }
}

impl Default for PythonPhysicalCodec {
    fn default() -> Self {
        Self::with_default_inner()
    }
}

impl PhysicalExtensionCodec for PythonPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        if try_encode_python_scalar_udf(node, buf)? {
            return Ok(());
        }
        self.inner.try_encode_udf(node, buf)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        match try_decode_python_scalar_udf(buf)? {
            Some(udf) => Ok(udf),
            None => self.inner.try_decode_udf(name, buf),
        }
    }
}

/// Encode a Python scalar UDF inline if `node` is one. Returns `Ok(true)`
/// when the payload (magic prefix + cloudpickle tuple) was written, or
/// `Ok(false)` to signal the caller should delegate to its `inner` codec
/// (non-Python UDF, e.g. built-in or FFI capsule).
pub(crate) fn try_encode_python_scalar_udf(node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<bool> {
    let Some(py_udf) = node
        .inner()
        .as_any()
        .downcast_ref::<PythonFunctionScalarUDF>()
    else {
        return Ok(false);
    };

    Python::attach(|py| -> Result<bool> {
        let bytes = encode_python_scalar_udf(py, py_udf)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        buf.extend_from_slice(PY_SCALAR_UDF_MAGIC);
        buf.extend_from_slice(&bytes);
        Ok(true)
    })
}

/// Decode an inline Python scalar UDF payload. Returns `Ok(None)` when
/// `buf` does not carry the magic prefix, signalling the caller should
/// delegate to its `inner` codec (which will typically defer to the
/// `FunctionRegistry`).
pub(crate) fn try_decode_python_scalar_udf(buf: &[u8]) -> Result<Option<Arc<ScalarUDF>>> {
    if buf.is_empty() || !buf.starts_with(PY_SCALAR_UDF_MAGIC) {
        return Ok(None);
    }
    let payload = &buf[PY_SCALAR_UDF_MAGIC.len()..];

    Python::attach(|py| -> Result<Option<Arc<ScalarUDF>>> {
        let udf = decode_python_scalar_udf(py, payload)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        Ok(Some(Arc::new(ScalarUDF::new_from_impl(udf))))
    })
}

/// Build the cloudpickle payload for a `PythonFunctionScalarUDF`.
///
/// Layout: `cloudpickle.dumps((name, func, input_schema_bytes,
/// return_field, volatility_str))`. Input fields ride along as an
/// IPC-encoded pyarrow Schema so they round-trip without extra plumbing.
fn encode_python_scalar_udf(py: Python<'_>, udf: &PythonFunctionScalarUDF) -> PyResult<Vec<u8>> {
    let cloudpickle = py.import("cloudpickle")?;

    let input_schema = Schema::new(udf.input_fields().to_vec());
    let pa_schema_obj = input_schema.to_pyarrow(py)?;
    let pa_schema = pa_schema_obj.into_bound();
    let schema_bytes: Vec<u8> = pa_schema
        .call_method0("serialize")?
        .call_method0("to_pybytes")?
        .extract()?;

    let return_field_obj = udf.return_field().as_ref().to_pyarrow(py)?;
    let volatility = format!("{:?}", udf.volatility()).to_lowercase();

    let payload = PyTuple::new(
        py,
        [
            udf.name().into_pyobject(py)?.into_any(),
            udf.func().bind(py).clone().into_any(),
            PyBytes::new(py, &schema_bytes).into_any(),
            return_field_obj.into_bound(),
            volatility.into_pyobject(py)?.into_any(),
        ],
    )?;

    let blob = cloudpickle.call_method1("dumps", (payload,))?;
    blob.extract::<Vec<u8>>()
}

/// Inverse of [`encode_python_scalar_udf`].
fn decode_python_scalar_udf(py: Python<'_>, payload: &[u8]) -> PyResult<PythonFunctionScalarUDF> {
    let cloudpickle = py.import("cloudpickle")?;
    let pyarrow = py.import("pyarrow")?;

    let tuple = cloudpickle
        .call_method1("loads", (PyBytes::new(py, payload),))?
        .cast_into::<PyTuple>()?;

    let name: String = tuple.get_item(0)?.extract()?;
    let func: Py<PyAny> = tuple.get_item(1)?.unbind();
    let schema_bytes: Vec<u8> = tuple.get_item(2)?.extract()?;
    let return_field_py = tuple.get_item(3)?;
    let volatility_str: String = tuple.get_item(4)?.extract()?;

    let buffer = pyarrow.call_method1("py_buffer", (PyBytes::new(py, &schema_bytes),))?;
    let pa_schema = pyarrow
        .getattr("ipc")?
        .call_method1("read_schema", (buffer,))?;

    let schema = Schema::from_pyarrow_bound(&pa_schema)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;
    let input_fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();

    let return_field = Field::from_pyarrow_bound(&return_field_py)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;

    let volatility = datafusion_python_util::parse_volatility(&volatility_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;

    Ok(PythonFunctionScalarUDF::from_parts(
        name,
        func,
        input_fields,
        return_field,
        volatility,
    ))
}
