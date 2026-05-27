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

use datafusion::common::Result;
use datafusion::execution::{TaskContext, TaskContextProvider};
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use datafusion_proto::physical_plan::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};
use datafusion_python_util::get_tokio_runtime;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

#[derive(Debug, Default)]
pub(crate) struct PhysicalCallCounters {
    pub encode_udf: AtomicUsize,
    pub decode_udf: AtomicUsize,
}

/// Mirror of [`super::logical_extension_codec::CountingLogicalExtensionCodec`]
/// for the physical layer. Delegates to `DefaultPhysicalExtensionCodec`
/// and bumps counters on UDF encode/decode so tests can prove the
/// session routed through a user-supplied physical codec.
#[derive(Debug)]
struct CountingPhysicalExtensionCodec {
    inner: DefaultPhysicalExtensionCodec,
    counters: Arc<PhysicalCallCounters>,
}

impl PhysicalExtensionCodec for CountingPhysicalExtensionCodec {
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
    name = "MyPhysicalExtensionCodec",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Clone)]
pub(crate) struct MyPhysicalExtensionCodec {
    counters: Arc<PhysicalCallCounters>,
}

#[pymethods]
impl MyPhysicalExtensionCodec {
    #[new]
    fn new() -> Self {
        Self {
            counters: Arc::new(PhysicalCallCounters::default()),
        }
    }

    fn encode_udf_calls(&self) -> usize {
        self.counters.encode_udf.load(Ordering::SeqCst)
    }

    fn decode_udf_calls(&self) -> usize {
        self.counters.decode_udf.load(Ordering::SeqCst)
    }

    fn __datafusion_physical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let inner: Arc<dyn PhysicalExtensionCodec + Send> =
            Arc::new(CountingPhysicalExtensionCodec {
                inner: DefaultPhysicalExtensionCodec {},
                counters: Arc::clone(&self.counters),
            });

        let runtime = get_tokio_runtime().handle().clone();
        let bare_session: Arc<SessionContext> = Arc::new(SessionContext::new());
        let ctx_provider = bare_session as Arc<dyn TaskContextProvider>;
        let ffi = FFI_PhysicalExtensionCodec::new(inner, Some(runtime), &ctx_provider);

        let name = cr"datafusion_physical_extension_codec".into();
        PyCapsule::new(py, ffi, Some(name))
    }
}
