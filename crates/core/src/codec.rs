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
//! Datafusion-python plans can carry references to Python-defined
//! objects that the upstream protobuf codecs do not know how to
//! serialize: pure-Python scalar / aggregate / window UDFs, Python
//! query-planning extensions, and so on. Their state lives inside
//! `Py<PyAny>` callables and closures rather than being recoverable
//! from a name in the receiver's function registry. To ship a plan
//! across a process boundary (pickle, `multiprocessing`, Ray actor,
//! `datafusion-distributed`, etc.) those payloads have to be encoded
//! into the proto wire format itself.
//!
//! [`PythonLogicalCodec`] is the [`LogicalExtensionCodec`] that
//! datafusion-python parks on every `SessionContext`. It wraps a
//! user-supplied (or default) inner codec and adds Python-aware
//! in-band encoding on top: when the encoder sees a Python-defined
//! UDF, the codec cloudpickles the callable + signature into the
//! `fun_definition` proto field; when the decoder sees a payload it
//! produced, it reconstructs the UDF from the bytes alone — no
//! pre-registration on the receiver. UDFs the codec does not
//! recognise are delegated to `inner`, which is typically
//! `DefaultLogicalExtensionCodec` but may be a downstream-supplied
//! FFI codec installed via
//! `SessionContext.with_logical_extension_codec(...)`.
//!
//! [`PythonPhysicalCodec`] is the symmetric wrapper around
//! [`PhysicalExtensionCodec`]. Logical and physical layers each have
//! a `try_encode_udf` / `try_decode_udf` pair, so a `ScalarUDF`
//! referenced inside a `LogicalPlan`, an `ExecutionPlan`, or a
//! `PhysicalExpr` must encode identically through either layer for
//! plans to survive a serialization round-trip. Both codecs share
//! the same payload framing for that reason.
//!
//! Payloads emitted by these codecs are tagged with an 8-byte magic
//! prefix so the decoder can distinguish them from arbitrary bytes
//! (empty `fun_definition` from the default codec, user FFI payloads
//! that picked a non-colliding prefix). Dispatch precedence on
//! decode: **Python-inline payload (magic prefix match) → `inner`
//! codec → caller's `FunctionRegistry` fallback.**
//!
//! ## Wire-format magic prefix registry
//!
//! | Layer + kind                  | Magic prefix |
//! | ----------------------------- | ------------ |
//! | `PythonLogicalCodec` scalar   | `DFPYUDF1`   |
//! | `PythonLogicalCodec` agg      | `DFPYUDA1`   |
//! | `PythonLogicalCodec` window   | `DFPYUDW1`   |
//! | `PythonPhysicalCodec` scalar  | `DFPYUDF1`   |
//! | `PythonPhysicalCodec` agg     | `DFPYUDA1`   |
//! | `PythonPhysicalCodec` window  | `DFPYUDW1`   |
//! | `PythonPhysicalCodec` expr    | `DFPYPE1`    |
//! | User FFI extension codec      | user-chosen  |
//! | Default codec                 | (none)       |
//!
//! Downstream FFI codecs should pick non-colliding prefixes (use a
//! `DF` namespace plus a crate-specific suffix). The codec
//! implementations in this module currently delegate every method to
//! `inner`; the encoder/decoder hooks for each kind are added as the
//! corresponding Python-side type becomes serializable.

use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use datafusion::common::{Result, TableReference};
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    AggregateUDF, AggregateUDFImpl, Extension, LogicalPlan, ScalarUDF, ScalarUDFImpl,
    TypeSignature, Volatility, WindowUDF, WindowUDFImpl,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::{DefaultLogicalExtensionCodec, LogicalExtensionCodec};
use datafusion_proto::physical_plan::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

use crate::udaf::PythonFunctionAggregateUDF;
use crate::udf::PythonFunctionScalarUDF;
use crate::udwf::PythonFunctionWindowUDF;

/// Wire-format prefix that tags a `fun_definition` payload as an
/// inlined Python scalar UDF (cloudpickled tuple of name, callable,
/// input schema, return field, volatility). Defined once here so
/// the encoder and decoder cannot drift.
pub(crate) const PY_SCALAR_UDF_MAGIC: &[u8] = b"DFPYUDF1";

/// Wire-format prefix for an inlined Python aggregate UDF
/// (cloudpickled tuple of name, accumulator factory, input schema,
/// return type, state types schema, volatility).
pub(crate) const PY_AGG_UDF_MAGIC: &[u8] = b"DFPYUDA1";

/// Wire-format prefix for an inlined Python window UDF (cloudpickled
/// tuple of name, evaluator factory, input schema, return type,
/// volatility).
pub(crate) const PY_WINDOW_UDF_MAGIC: &[u8] = b"DFPYUDW1";

/// `LogicalExtensionCodec` parked on every `SessionContext`. Holds
/// the Python-aware encoding hooks for logical-layer types
/// (`LogicalPlan`, `Expr`) and delegates everything it does not
/// handle to the composable `inner` codec — typically
/// `DefaultLogicalExtensionCodec`, or a downstream FFI codec
/// installed via `SessionContext.with_logical_extension_codec(...)`.
///
/// Sitting at the top of the session's logical codec stack means
/// every serializer that reads `session.logical_codec()` automatically
/// picks up Python-aware encoding for free.
#[derive(Debug)]
pub struct PythonLogicalCodec {
    inner: Arc<dyn LogicalExtensionCodec>,
    python_udf_inlining: bool,
}

impl PythonLogicalCodec {
    pub fn new(inner: Arc<dyn LogicalExtensionCodec>) -> Self {
        Self {
            inner,
            python_udf_inlining: true,
        }
    }

    pub fn inner(&self) -> &Arc<dyn LogicalExtensionCodec> {
        &self.inner
    }

    /// Whether Python-defined UDFs are encoded inline (and decoded
    /// from cloudpickle blobs). Defaults to `true`. Set to `false`
    /// when the codec sits on a session that must produce
    /// cross-language wire bytes, or reject `cloudpickle.loads` on
    /// untrusted `from_bytes` input.
    pub fn with_python_udf_inlining(mut self, enabled: bool) -> Self {
        self.python_udf_inlining = enabled;
        self
    }

    pub fn python_udf_inlining(&self) -> bool {
        self.python_udf_inlining
    }
}

impl Default for PythonLogicalCodec {
    fn default() -> Self {
        Self::new(Arc::new(DefaultLogicalExtensionCodec {}))
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

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn FileFormatFactory>> {
        self.inner.try_decode_file_format(buf, ctx)
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> Result<()> {
        self.inner.try_encode_file_format(buf, node)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        if self.python_udf_inlining && try_encode_python_scalar_udf(node, buf)? {
            return Ok(());
        }
        self.inner.try_encode_udf(node, buf)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        if self.python_udf_inlining {
            if let Some(udf) = try_decode_python_scalar_udf(buf)? {
                return Ok(udf);
            }
        } else if buf.starts_with(PY_SCALAR_UDF_MAGIC) {
            return Err(refuse_inline_payload("scalar UDF", name));
        }
        self.inner.try_decode_udf(name, buf)
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        if self.python_udf_inlining && try_encode_python_agg_udf(node, buf)? {
            return Ok(());
        }
        self.inner.try_encode_udaf(node, buf)
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        if self.python_udf_inlining {
            if let Some(udaf) = try_decode_python_agg_udf(buf)? {
                return Ok(udaf);
            }
        } else if buf.starts_with(PY_AGG_UDF_MAGIC) {
            return Err(refuse_inline_payload("aggregate UDF", name));
        }
        self.inner.try_decode_udaf(name, buf)
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        if self.python_udf_inlining && try_encode_python_window_udf(node, buf)? {
            return Ok(());
        }
        self.inner.try_encode_udwf(node, buf)
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        if self.python_udf_inlining {
            if let Some(udwf) = try_decode_python_window_udf(buf)? {
                return Ok(udwf);
            }
        } else if buf.starts_with(PY_WINDOW_UDF_MAGIC) {
            return Err(refuse_inline_payload("window UDF", name));
        }
        self.inner.try_decode_udwf(name, buf)
    }
}

/// Build the error returned by a strict codec when it receives an
/// inline Python-UDF payload it has been told not to deserialize.
fn refuse_inline_payload(kind: &str, name: &str) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Plan(format!(
        "Refusing to deserialize inline Python {kind} '{name}': Python UDF \
         inlining is disabled on this session. Re-encode the bytes with \
         inlining enabled, or register '{name}' on the sender's session \
         before encode (and on the receiver before decode) so the UDF \
         travels by name."
    ))
}

/// `PhysicalExtensionCodec` mirror of [`PythonLogicalCodec`] parked
/// on the same `SessionContext`. Carries the Python-aware encoding
/// hooks for physical-layer types (`ExecutionPlan`, `PhysicalExpr`)
/// and delegates the rest to `inner`.
///
/// The `PhysicalExtensionCodec` trait has its own `try_encode_udf`
/// / `try_decode_udf` pair distinct from the logical one, so a
/// `ScalarUDF` referenced inside a physical plan needs Python-aware
/// encoding on this layer too — otherwise a plan with a Python UDF
/// would round-trip at the logical level but break at the physical
/// level. Both layers reuse the shared payload framing
/// ([`PY_SCALAR_UDF_MAGIC`] et al.) so the wire format is identical.
#[derive(Debug)]
pub struct PythonPhysicalCodec {
    inner: Arc<dyn PhysicalExtensionCodec>,
    python_udf_inlining: bool,
}

impl PythonPhysicalCodec {
    pub fn new(inner: Arc<dyn PhysicalExtensionCodec>) -> Self {
        Self {
            inner,
            python_udf_inlining: true,
        }
    }

    pub fn inner(&self) -> &Arc<dyn PhysicalExtensionCodec> {
        &self.inner
    }

    /// See [`PythonLogicalCodec::with_python_udf_inlining`].
    pub fn with_python_udf_inlining(mut self, enabled: bool) -> Self {
        self.python_udf_inlining = enabled;
        self
    }

    pub fn python_udf_inlining(&self) -> bool {
        self.python_udf_inlining
    }
}

impl Default for PythonPhysicalCodec {
    fn default() -> Self {
        Self::new(Arc::new(DefaultPhysicalExtensionCodec {}))
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
        if self.python_udf_inlining && try_encode_python_scalar_udf(node, buf)? {
            return Ok(());
        }
        self.inner.try_encode_udf(node, buf)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        if self.python_udf_inlining {
            if let Some(udf) = try_decode_python_scalar_udf(buf)? {
                return Ok(udf);
            }
        } else if buf.starts_with(PY_SCALAR_UDF_MAGIC) {
            return Err(refuse_inline_payload("scalar UDF", name));
        }
        self.inner.try_decode_udf(name, buf)
    }

    fn try_encode_expr(&self, node: &Arc<dyn PhysicalExpr>, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_expr(node, buf)
    }

    fn try_decode_expr(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.inner.try_decode_expr(buf, inputs)
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        if self.python_udf_inlining && try_encode_python_agg_udf(node, buf)? {
            return Ok(());
        }
        self.inner.try_encode_udaf(node, buf)
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        if self.python_udf_inlining {
            if let Some(udaf) = try_decode_python_agg_udf(buf)? {
                return Ok(udaf);
            }
        } else if buf.starts_with(PY_AGG_UDF_MAGIC) {
            return Err(refuse_inline_payload("aggregate UDF", name));
        }
        self.inner.try_decode_udaf(name, buf)
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        if self.python_udf_inlining && try_encode_python_window_udf(node, buf)? {
            return Ok(());
        }
        self.inner.try_encode_udwf(node, buf)
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        if self.python_udf_inlining {
            if let Some(udwf) = try_decode_python_window_udf(buf)? {
                return Ok(udwf);
            }
        } else if buf.starts_with(PY_WINDOW_UDF_MAGIC) {
            return Err(refuse_inline_payload("window UDF", name));
        }
        self.inner.try_decode_udwf(name, buf)
    }
}

// =============================================================================
// Shared Python scalar UDF encode / decode helpers
//
// Both `PythonLogicalCodec` and `PythonPhysicalCodec` consult these on
// every `try_encode_udf` / `try_decode_udf` call. Same wire format on
// both layers — a Python `ScalarUDF` referenced inside a `LogicalPlan`
// or an `ExecutionPlan` round-trips identically.
// =============================================================================

/// Encode a Python scalar UDF inline if `node` is one. Returns
/// `Ok(true)` when the payload (`DFPYUDF1` prefix + cloudpickled
/// tuple) was written and the caller should skip its inner codec.
/// Returns `Ok(false)` for any non-Python UDF, signalling the caller
/// to delegate to its `inner`.
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

/// Decode an inline Python scalar UDF payload. Returns `Ok(None)`
/// when `buf` does not carry the `DFPYUDF1` prefix, signalling the
/// caller to delegate to its `inner` codec (and eventually the
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
/// return_schema_bytes, volatility_str))`. Both schema blobs are
/// produced by arrow-rs's native IPC stream writer — no pyarrow
/// round-trip — and decoded with the matching stream reader on the
/// receiver. Input `DataType`s come from the UDF's `Signature`
/// (always `TypeSignature::Exact` for Python-defined UDFs);
/// receiver-side `Field` metadata is irrelevant because
/// `from_parts` immediately collapses incoming `Field`s back to
/// `DataType`s for the reconstructed `Signature`.
fn encode_python_scalar_udf(py: Python<'_>, udf: &PythonFunctionScalarUDF) -> PyResult<Vec<u8>> {
    let cloudpickle = py.import("cloudpickle")?;

    let signature = udf.signature();
    let input_dtypes: Vec<arrow::datatypes::DataType> = match &signature.type_signature {
        TypeSignature::Exact(types) => types.clone(),
        other => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "PythonFunctionScalarUDF expected Signature::Exact, got {other:?}"
            )));
        }
    };
    let input_fields: Vec<Field> = input_dtypes
        .into_iter()
        .enumerate()
        .map(|(i, dt)| Field::new(format!("arg_{i}"), dt, true))
        .collect();
    let input_schema = Schema::new(input_fields);
    let input_schema_bytes = schema_to_ipc_bytes(&input_schema)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;

    let return_schema = Schema::new(vec![udf.return_field().as_ref().clone()]);
    let return_schema_bytes = schema_to_ipc_bytes(&return_schema)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;

    let volatility = volatility_wire_str(signature.volatility);

    let payload = PyTuple::new(
        py,
        [
            udf.name().into_pyobject(py)?.into_any(),
            udf.func().bind(py).clone().into_any(),
            PyBytes::new(py, &input_schema_bytes).into_any(),
            PyBytes::new(py, &return_schema_bytes).into_any(),
            volatility.into_pyobject(py)?.into_any(),
        ],
    )?;

    let blob = cloudpickle.call_method1("dumps", (payload,))?;
    blob.extract::<Vec<u8>>()
}

/// Inverse of [`encode_python_scalar_udf`].
fn decode_python_scalar_udf(py: Python<'_>, payload: &[u8]) -> PyResult<PythonFunctionScalarUDF> {
    let cloudpickle = py.import("cloudpickle")?;

    let tuple = cloudpickle
        .call_method1("loads", (PyBytes::new(py, payload),))?
        .cast_into::<PyTuple>()?;

    let name: String = tuple.get_item(0)?.extract()?;
    let func: Py<PyAny> = tuple.get_item(1)?.unbind();
    let input_schema_bytes: Vec<u8> = tuple.get_item(2)?.extract()?;
    let return_schema_bytes: Vec<u8> = tuple.get_item(3)?.extract()?;
    let volatility_str: String = tuple.get_item(4)?.extract()?;

    let input_schema = schema_from_ipc_bytes(&input_schema_bytes)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;
    let input_fields: Vec<Field> = input_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();

    let return_schema = schema_from_ipc_bytes(&return_schema_bytes)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;
    let return_field = return_schema
        .fields()
        .first()
        .ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "PythonFunctionScalarUDF return schema must contain exactly one field",
            )
        })?
        .as_ref()
        .clone();

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

/// Serialize a `Schema` to a self-contained IPC stream containing
/// only the schema message (no record batches). Inverse:
/// [`schema_from_ipc_bytes`].
fn schema_to_ipc_bytes(schema: &Schema) -> arrow::error::Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Decode an IPC stream containing only a schema message back into a
/// `Schema`. Inverse: [`schema_to_ipc_bytes`].
fn schema_from_ipc_bytes(bytes: &[u8]) -> arrow::error::Result<Schema> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    Ok(reader.schema().as_ref().clone())
}

/// Stable wire-format string for a `Volatility`. Pinned to the three
/// tokens [`datafusion_python_util::parse_volatility`] accepts, so an
/// upstream change to `Volatility`'s `Debug` repr cannot silently
/// produce bytes the decoder rejects.
fn volatility_wire_str(v: Volatility) -> &'static str {
    match v {
        Volatility::Immutable => "immutable",
        Volatility::Stable => "stable",
        Volatility::Volatile => "volatile",
    }
}

// =============================================================================
// Shared Python window UDF encode / decode helpers
//
// Cloudpickle tuple shape: `(name, evaluator_factory, input_schema_bytes,
// return_schema_bytes, volatility_str)`. The evaluator factory is the
// Python callable that produces a new evaluator instance per partition.
// =============================================================================

pub(crate) fn try_encode_python_window_udf(node: &WindowUDF, buf: &mut Vec<u8>) -> Result<bool> {
    let Some(py_udf) = node
        .inner()
        .as_any()
        .downcast_ref::<PythonFunctionWindowUDF>()
    else {
        return Ok(false);
    };

    Python::attach(|py| -> Result<bool> {
        let bytes = encode_python_window_udf(py, py_udf)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        buf.extend_from_slice(PY_WINDOW_UDF_MAGIC);
        buf.extend_from_slice(&bytes);
        Ok(true)
    })
}

pub(crate) fn try_decode_python_window_udf(buf: &[u8]) -> Result<Option<Arc<WindowUDF>>> {
    if buf.is_empty() || !buf.starts_with(PY_WINDOW_UDF_MAGIC) {
        return Ok(None);
    }
    let payload = &buf[PY_WINDOW_UDF_MAGIC.len()..];

    Python::attach(|py| -> Result<Option<Arc<WindowUDF>>> {
        let udf = decode_python_window_udf(py, payload)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        Ok(Some(Arc::new(WindowUDF::new_from_impl(udf))))
    })
}

fn encode_python_window_udf(py: Python<'_>, udf: &PythonFunctionWindowUDF) -> PyResult<Vec<u8>> {
    let cloudpickle = py.import("cloudpickle")?;

    let signature = WindowUDFImpl::signature(udf);
    let input_dtypes: Vec<arrow::datatypes::DataType> = match &signature.type_signature {
        TypeSignature::Exact(types) => types.clone(),
        other => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "PythonFunctionWindowUDF expected Signature::Exact, got {other:?}"
            )));
        }
    };
    let input_fields: Vec<Field> = input_dtypes
        .into_iter()
        .enumerate()
        .map(|(i, dt)| Field::new(format!("arg_{i}"), dt, true))
        .collect();
    let input_schema = Schema::new(input_fields);
    let input_schema_bytes = schema_to_ipc_bytes(&input_schema)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;

    let return_schema = Schema::new(vec![Field::new("result", udf.return_type().clone(), true)]);
    let return_schema_bytes = schema_to_ipc_bytes(&return_schema)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;

    let volatility = volatility_wire_str(signature.volatility);

    let payload = PyTuple::new(
        py,
        [
            WindowUDFImpl::name(udf).into_pyobject(py)?.into_any(),
            udf.evaluator().bind(py).clone().into_any(),
            PyBytes::new(py, &input_schema_bytes).into_any(),
            PyBytes::new(py, &return_schema_bytes).into_any(),
            volatility.into_pyobject(py)?.into_any(),
        ],
    )?;

    let blob = cloudpickle.call_method1("dumps", (payload,))?;
    blob.extract::<Vec<u8>>()
}

fn decode_python_window_udf(py: Python<'_>, payload: &[u8]) -> PyResult<PythonFunctionWindowUDF> {
    let cloudpickle = py.import("cloudpickle")?;

    let tuple = cloudpickle
        .call_method1("loads", (PyBytes::new(py, payload),))?
        .cast_into::<PyTuple>()?;

    let name: String = tuple.get_item(0)?.extract()?;
    let evaluator: Py<PyAny> = tuple.get_item(1)?.unbind();
    let input_schema_bytes: Vec<u8> = tuple.get_item(2)?.extract()?;
    let return_schema_bytes: Vec<u8> = tuple.get_item(3)?.extract()?;
    let volatility_str: String = tuple.get_item(4)?.extract()?;

    let input_schema = schema_from_ipc_bytes(&input_schema_bytes)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;
    let input_types: Vec<arrow::datatypes::DataType> = input_schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect();

    let return_schema = schema_from_ipc_bytes(&return_schema_bytes)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;
    let return_type = return_schema
        .fields()
        .first()
        .ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "PythonFunctionWindowUDF return schema must contain exactly one field",
            )
        })?
        .data_type()
        .clone();

    let volatility = datafusion_python_util::parse_volatility(&volatility_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;

    Ok(PythonFunctionWindowUDF::from_parts(
        name,
        evaluator,
        input_types,
        return_type,
        volatility,
    ))
}

// =============================================================================
// Shared Python aggregate UDF encode / decode helpers
//
// Cloudpickle tuple shape: `(name, accumulator_factory, input_schema_bytes,
// return_type_bytes, state_schema_bytes, volatility_str)`. The accumulator
// factory is the Python callable that produces a new accumulator instance
// per partition.
// =============================================================================

pub(crate) fn try_encode_python_agg_udf(node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<bool> {
    let Some(py_udf) = node
        .inner()
        .as_any()
        .downcast_ref::<PythonFunctionAggregateUDF>()
    else {
        return Ok(false);
    };

    Python::attach(|py| -> Result<bool> {
        let bytes = encode_python_agg_udf(py, py_udf)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        buf.extend_from_slice(PY_AGG_UDF_MAGIC);
        buf.extend_from_slice(&bytes);
        Ok(true)
    })
}

pub(crate) fn try_decode_python_agg_udf(buf: &[u8]) -> Result<Option<Arc<AggregateUDF>>> {
    if buf.is_empty() || !buf.starts_with(PY_AGG_UDF_MAGIC) {
        return Ok(None);
    }
    let payload = &buf[PY_AGG_UDF_MAGIC.len()..];

    Python::attach(|py| -> Result<Option<Arc<AggregateUDF>>> {
        let udf = decode_python_agg_udf(py, payload)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        Ok(Some(Arc::new(AggregateUDF::new_from_impl(udf))))
    })
}

fn encode_python_agg_udf(py: Python<'_>, udf: &PythonFunctionAggregateUDF) -> PyResult<Vec<u8>> {
    let cloudpickle = py.import("cloudpickle")?;

    let signature = AggregateUDFImpl::signature(udf);
    let input_dtypes: Vec<arrow::datatypes::DataType> = match &signature.type_signature {
        TypeSignature::Exact(types) => types.clone(),
        other => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "PythonFunctionAggregateUDF expected Signature::Exact, got {other:?}"
            )));
        }
    };
    let input_fields: Vec<Field> = input_dtypes
        .into_iter()
        .enumerate()
        .map(|(i, dt)| Field::new(format!("arg_{i}"), dt, true))
        .collect();
    let input_schema_bytes = schema_to_ipc_bytes(&Schema::new(input_fields))
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;

    let return_schema = Schema::new(vec![Field::new("result", udf.return_type().clone(), true)]);
    let return_schema_bytes = schema_to_ipc_bytes(&return_schema)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;

    let state_fields: Vec<Field> = udf
        .state_fields_ref()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let state_schema_bytes = schema_to_ipc_bytes(&Schema::new(state_fields))
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;

    let volatility = volatility_wire_str(signature.volatility);

    let payload = PyTuple::new(
        py,
        [
            AggregateUDFImpl::name(udf).into_pyobject(py)?.into_any(),
            udf.accumulator().bind(py).clone().into_any(),
            PyBytes::new(py, &input_schema_bytes).into_any(),
            PyBytes::new(py, &return_schema_bytes).into_any(),
            PyBytes::new(py, &state_schema_bytes).into_any(),
            volatility.into_pyobject(py)?.into_any(),
        ],
    )?;

    let blob = cloudpickle.call_method1("dumps", (payload,))?;
    blob.extract::<Vec<u8>>()
}

fn decode_python_agg_udf(py: Python<'_>, payload: &[u8]) -> PyResult<PythonFunctionAggregateUDF> {
    let cloudpickle = py.import("cloudpickle")?;

    let tuple = cloudpickle
        .call_method1("loads", (PyBytes::new(py, payload),))?
        .cast_into::<PyTuple>()?;

    let name: String = tuple.get_item(0)?.extract()?;
    let accumulator: Py<PyAny> = tuple.get_item(1)?.unbind();
    let input_schema_bytes: Vec<u8> = tuple.get_item(2)?.extract()?;
    let return_schema_bytes: Vec<u8> = tuple.get_item(3)?.extract()?;
    let state_schema_bytes: Vec<u8> = tuple.get_item(4)?.extract()?;
    let volatility_str: String = tuple.get_item(5)?.extract()?;

    let input_schema = schema_from_ipc_bytes(&input_schema_bytes)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;
    let input_types: Vec<arrow::datatypes::DataType> = input_schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect();

    let return_schema = schema_from_ipc_bytes(&return_schema_bytes)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;
    let return_type = return_schema
        .fields()
        .first()
        .ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "PythonFunctionAggregateUDF return schema must contain exactly one field",
            )
        })?
        .data_type()
        .clone();

    let state_schema = schema_from_ipc_bytes(&state_schema_bytes)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;
    // Preserve the encoded state field metadata (names, nullability,
    // arbitrary key/value attributes) so the post-decode UDF reports
    // the same state schema as the sender's instance — important for
    // accumulators whose `StateFieldsArgs` consumers key off names or
    // nullability rather than positional `DataType`.
    let state_fields: Vec<arrow::datatypes::FieldRef> =
        state_schema.fields().iter().cloned().collect();

    let volatility = datafusion_python_util::parse_volatility(&volatility_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;

    Ok(PythonFunctionAggregateUDF::from_parts(
        name,
        accumulator,
        input_types,
        return_type,
        state_fields,
        volatility,
    ))
}
