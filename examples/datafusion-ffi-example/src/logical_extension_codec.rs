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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::SchemaRef;
use datafusion::common::{Result, TableReference};
use datafusion::datasource::TableProvider;
use datafusion::execution::{TaskContext, TaskContextProvider};
use datafusion::logical_expr::{Extension, LogicalPlan, ScalarUDF};
use datafusion::prelude::SessionContext;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_proto::logical_plan::{DefaultLogicalExtensionCodec, LogicalExtensionCodec};
use datafusion_python_util::get_tokio_runtime;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

/// Tracks how often each `try_*_udf` entry point fires. Surface for
/// Python tests to assert the session routed UDF
/// encode/decode through this user-supplied codec rather than the
/// upstream default.
#[derive(Debug, Default)]
pub(crate) struct CallCounters {
    pub encode_udf: AtomicUsize,
    pub decode_udf: AtomicUsize,
}

/// Minimal user-supplied `LogicalExtensionCodec` for integration tests.
/// Delegates everything to `DefaultLogicalExtensionCodec` and bumps
/// counters on the UDF entry points so tests can prove the wrapper
/// installed via `SessionContext.with_logical_extension_codec(...)`
/// actually gets consulted.
#[derive(Debug)]
struct CountingLogicalExtensionCodec {
    inner: DefaultLogicalExtensionCodec,
    counters: Arc<CallCounters>,
}

impl LogicalExtensionCodec for CountingLogicalExtensionCodec {
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

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.counters.decode_udf.fetch_add(1, Ordering::SeqCst);
        self.inner.try_decode_udf(name, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.counters.encode_udf.fetch_add(1, Ordering::SeqCst);
        self.inner.try_encode_udf(node, buf)
    }
}

#[pyclass(
    from_py_object,
    name = "MyLogicalExtensionCodec",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Clone)]
pub(crate) struct MyLogicalExtensionCodec {
    counters: Arc<CallCounters>,
}

#[pymethods]
impl MyLogicalExtensionCodec {
    #[new]
    fn new() -> Self {
        Self {
            counters: Arc::new(CallCounters::default()),
        }
    }

    /// Number of `try_encode_udf` invocations observed since
    /// construction.
    fn encode_udf_calls(&self) -> usize {
        self.counters.encode_udf.load(Ordering::SeqCst)
    }

    /// Number of `try_decode_udf` invocations observed.
    fn decode_udf_calls(&self) -> usize {
        self.counters.decode_udf.load(Ordering::SeqCst)
    }

    /// Capsule entry point consumed by
    /// `datafusion_python_util::ffi_logical_codec_from_pycapsule`.
    /// datafusion-python invokes this with no arguments when the user
    /// calls `ctx.with_logical_extension_codec(my_codec)`. The codec
    /// owns its own bare `SessionContext` as a TaskContextProvider —
    /// good enough for tests that only exercise UDF encode/decode.
    fn __datafusion_logical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let inner: Arc<dyn LogicalExtensionCodec> = Arc::new(CountingLogicalExtensionCodec {
            inner: DefaultLogicalExtensionCodec {},
            counters: Arc::clone(&self.counters),
        });

        let runtime = get_tokio_runtime().handle().clone();
        let bare_session: Arc<SessionContext> = Arc::new(SessionContext::new());
        let ctx_provider = bare_session as Arc<dyn TaskContextProvider>;
        let ffi = FFI_LogicalExtensionCodec::new(inner, Some(runtime), &ctx_provider);

        let name = cr"datafusion_logical_extension_codec".into();
        PyCapsule::new(py, ffi, Some(name))
    }
}
