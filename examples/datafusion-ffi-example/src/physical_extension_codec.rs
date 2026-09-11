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

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use datafusion_python_util::{ffi_task_context_provider_from_pycapsule, get_tokio_runtime};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::foreign_plan_workaround;
use crate::required_udf::{TaskContextProbe, resolve_required_udf};

#[derive(Debug, Default)]
pub(crate) struct PhysicalCallCounters {
    pub encode_udf: AtomicUsize,
    pub decode_udf: AtomicUsize,
    pub encode_execution_plan: AtomicUsize,
    pub decode_execution_plan: AtomicUsize,
    pub task_ctx: TaskContextProbe,
}

/// Physical companion to the logical example codec.
///
/// Provider-owned memory scan plans use a same-process token registry so the
/// owning cdylib can restore their concrete Rust type after the plan travels
/// through the independent query-planner and datafusion-python libraries.
///
/// See [`execution_plans`] for the token lifecycle, which is narrower than it
/// looks: a decode consumes its token, so the same encoded plan cannot be
/// decoded twice.
struct CountingPhysicalExtensionCodec {
    inner: DefaultPhysicalExtensionCodec,
    counters: Arc<PhysicalCallCounters>,
    /// Scalar function every decode call must resolve from the `TaskContext`
    /// it is handed. See [`crate::required_udf`].
    required_udf: Option<String>,
}

impl fmt::Debug for CountingPhysicalExtensionCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CountingPhysicalExtensionCodec")
            .field("inner", &self.inner)
            .field("counters", &self.counters)
            .finish_non_exhaustive()
    }
}

impl PhysicalExtensionCodec for CountingPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        resolve_required_udf(self.required_udf.as_deref(), ctx, &self.counters.task_ctx)?;
        if let Some(plan) = foreign_plan_workaround::take(buf)? {
            self.counters
                .decode_execution_plan
                .fetch_add(1, Ordering::SeqCst);
            return Ok(plan);
        }
        self.inner.try_decode(buf, inputs, ctx, proto_converter)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        if foreign_plan_workaround::claims(&node) {
            self.counters
                .encode_execution_plan
                .fetch_add(1, Ordering::SeqCst);
            foreign_plan_workaround::park(node, buf)?;
            return Ok(());
        }
        self.inner.try_encode(node, buf, proto_converter)
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
    required_udf: Option<String>,
}

#[pymethods]
impl MyPhysicalExtensionCodec {
    /// Build the codec.
    ///
    /// `require_udf_on_decode` names a scalar function that every decode call
    /// must find in the `TaskContext` it is handed. Leave it unset for the
    /// ordinary behaviour; set it to observe *which* session's registry the
    /// FFI decode callback actually receives.
    #[new]
    #[pyo3(signature = (require_udf_on_decode=None))]
    fn new(require_udf_on_decode: Option<String>) -> Self {
        Self {
            counters: Arc::new(PhysicalCallCounters::default()),
            required_udf: require_udf_on_decode,
        }
    }

    /// Number of decode calls that resolved `require_udf_on_decode`.
    fn task_context_udf_resolutions(&self) -> usize {
        self.counters.task_ctx.resolutions()
    }

    /// Session id of the `TaskContext` the most recent decode callback ran
    /// against, or `None` before any decode.
    fn last_task_context_session_id(&self) -> Option<String> {
        self.counters.task_ctx.last_session_id()
    }

    fn encode_udf_calls(&self) -> usize {
        self.counters.encode_udf.load(Ordering::SeqCst)
    }

    fn decode_udf_calls(&self) -> usize {
        self.counters.decode_udf.load(Ordering::SeqCst)
    }

    fn execution_plan_encode_calls(&self) -> usize {
        self.counters.encode_execution_plan.load(Ordering::SeqCst)
    }

    fn execution_plan_decode_calls(&self) -> usize {
        self.counters.decode_execution_plan.load(Ordering::SeqCst)
    }

    /// Export the codec, bound to the session it is being installed on.
    ///
    /// See [`crate::logical_extension_codec::MyLogicalExtensionCodec`] for why
    /// `session` is taken rather than a context this library invents.
    fn __datafusion_physical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let inner: Arc<dyn PhysicalExtensionCodec + Send> =
            Arc::new(CountingPhysicalExtensionCodec {
                inner: DefaultPhysicalExtensionCodec {},
                counters: Arc::clone(&self.counters),
                required_udf: self.required_udf.clone(),
            });

        let runtime = get_tokio_runtime().handle().clone();
        let ctx_provider = ffi_task_context_provider_from_pycapsule(&session)?;
        let ffi = FFI_PhysicalExtensionCodec::new(inner, Some(runtime), ctx_provider);

        PyCapsule::new_with_value(py, ffi, cr"datafusion_physical_extension_codec")
    }
}
