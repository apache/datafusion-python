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

//! A codec whose functions need no payload at all.
//!
//! Most extension codecs answer with bytes. This one owns a fixed catalog of
//! functions that are fully described by their names, so `try_encode_udf`
//! writes nothing and `try_decode_udf` rebuilds the function from `name`
//! alone. DataFusion supports that shape directly: an encoder that writes no
//! bytes leaves `fun_definition` unset, and the decoder then tries the
//! `FunctionRegistry` first and the codec second — see the
//! `None => ctx.udf(..).or_else(|_| codec.try_decode_udf(name, &[]))` arm in
//! `datafusion-proto`'s `from_proto.rs`.
//!
//! It exists here to pin that arm. Because there are no bytes, there is
//! nothing to tag with the codec's identity, so this is the one path where
//! `PythonLogicalCodec` still offers a payload to every installed codec in
//! turn. A change that wrapped empty encodings in an envelope would set
//! `fun_definition`, skip the registry lookup permanently, and break both this
//! codec and plain by-name round trips — with no other test noticing.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_schema::DataType;
use datafusion::common::error::Result;
use datafusion::common::not_impl_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_proto::logical_plan::{DefaultLogicalExtensionCodec, LogicalExtensionCodec};
use datafusion_python_util::{ffi_task_context_provider_from_pycapsule, get_tokio_runtime};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

/// Prefix marking the functions this library owns. A name is the entire
/// encoding, so the prefix is the whole ownership test.
const NAME_PREFIX: &str = "name_only_";

/// Scalar function reconstructed purely from its name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NameOnlyUdf {
    name: String,
    signature: Signature,
}

impl NameOnlyUdf {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NameOnlyUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(args.args[0].clone())
    }
}

#[derive(Default)]
struct Counters {
    encode_udf: AtomicUsize,
    decode_udf: AtomicUsize,
}

impl fmt::Debug for Counters {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Counters").finish_non_exhaustive()
    }
}

struct NameOnlyLogicalExtensionCodec {
    inner: DefaultLogicalExtensionCodec,
    counters: Arc<Counters>,
}

impl fmt::Debug for NameOnlyLogicalExtensionCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NameOnlyLogicalExtensionCodec")
            .finish_non_exhaustive()
    }
}

impl LogicalExtensionCodec for NameOnlyLogicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        ctx: &datafusion::execution::TaskContext,
    ) -> Result<datafusion::logical_expr::Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &datafusion::common::TableReference,
        schema: arrow_schema::SchemaRef,
        ctx: &datafusion::execution::TaskContext,
    ) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &datafusion::common::TableReference,
        node: Arc<dyn datafusion::datasource::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.inner.try_encode_table_provider(table_ref, node, buf)
    }

    /// Writes nothing on purpose. The name is the whole encoding, so there is
    /// no payload to emit, and returning `Ok` with an empty buffer is how a
    /// codec says "encoded by name" to DataFusion.
    fn try_encode_udf(&self, node: &ScalarUDF, _buf: &mut Vec<u8>) -> Result<()> {
        if node.name().starts_with(NAME_PREFIX) {
            self.counters.encode_udf.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Rebuilds the function from `name`, with no registry entry and no bytes.
    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        if !name.starts_with(NAME_PREFIX) {
            return not_impl_err!("Not a name-only function: {name}");
        }
        if !buf.is_empty() {
            return not_impl_err!(
                "name-only functions carry no payload, but {} bytes were supplied for {name}",
                buf.len()
            );
        }
        self.counters.decode_udf.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(ScalarUDF::from(NameOnlyUdf::new(name))))
    }
}

/// The function [`NameOnlyUdfCodec`] owns, exported so a session can register
/// it and build a plan that references it.
///
/// Only the *encoding* session needs it registered. The decoding session
/// deliberately does not, which is what forces the codec's name-only decode
/// path to run.
#[pyclass(
    from_py_object,
    name = "NameOnlyFunction",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Debug, Clone)]
pub(crate) struct NameOnlyFunction;

#[pymethods]
impl NameOnlyFunction {
    #[new]
    fn new() -> Self {
        Self
    }

    fn __datafusion_scalar_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let func = Arc::new(ScalarUDF::from(NameOnlyUdf::new(format!(
            "{NAME_PREFIX}identity"
        ))));
        PyCapsule::new_with_value(
            py,
            datafusion_ffi::udf::FFI_ScalarUDF::from(func),
            cr"datafusion_scalar_udf",
        )
    }
}

/// Codec owning functions that are reconstructible from their names alone.
///
/// A real library shaped like this would be one shipping a fixed catalog of
/// built-ins: nothing about a call site varies, so there is nothing to encode.
#[pyclass(
    from_py_object,
    name = "NameOnlyUdfCodec",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Clone)]
pub(crate) struct NameOnlyUdfCodec {
    counters: Arc<Counters>,
}

#[pymethods]
impl NameOnlyUdfCodec {
    #[new]
    fn new() -> Self {
        Self {
            counters: Arc::new(Counters::default()),
        }
    }

    /// Name of the function this codec can rebuild, for use in a query.
    #[staticmethod]
    fn function_name() -> String {
        format!("{NAME_PREFIX}identity")
    }

    fn encode_udf_calls(&self) -> usize {
        self.counters.encode_udf.load(Ordering::SeqCst)
    }

    fn decode_udf_calls(&self) -> usize {
        self.counters.decode_udf.load(Ordering::SeqCst)
    }

    fn __datafusion_logical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let inner: Arc<dyn LogicalExtensionCodec> = Arc::new(NameOnlyLogicalExtensionCodec {
            inner: DefaultLogicalExtensionCodec {},
            counters: Arc::clone(&self.counters),
        });

        let runtime = get_tokio_runtime().handle().clone();
        let ctx_provider = ffi_task_context_provider_from_pycapsule(&session)?;
        let ffi = FFI_LogicalExtensionCodec::new(inner, Some(runtime), ctx_provider);

        PyCapsule::new_with_value(py, ffi, cr"datafusion_logical_extension_codec")
    }
}
