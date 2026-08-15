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
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, TableReference};
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use datafusion_ffi::query_planner::FFI_QueryPlanner;
use datafusion_proto::logical_plan::{DefaultLogicalExtensionCodec, LogicalExtensionCodec};
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use datafusion_python_util::get_tokio_runtime;
use datafusion_session::QueryPlanner;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict};

use crate::planner::{DistributedQueryPlanner, PlannerObservations, planner_config_from_options};

/// Values of `ffi_query_planner.max_rows` observed through the task-context
/// provider bound at installation time. Decoding reads the provider's current
/// session state, so these prove which context the provider targets.
type ObservedMaxRows = Arc<Mutex<Vec<usize>>>;

fn record_task_ctx(observed: &ObservedMaxRows, ctx: &TaskContext) {
    if let Ok(config) = planner_config_from_options(ctx.session_config().options())
        && let Ok(mut observed) = observed.lock()
    {
        observed.push(config.max_rows);
    }
}

/// Records the task context resolved by the FFI wrapper, then declines by
/// delegating to the default codec so the host's codec chain falls through to
/// the codec that owns the payload.
struct ObservingLogicalExtensionCodec {
    inner: DefaultLogicalExtensionCodec,
    observed: ObservedMaxRows,
}

impl fmt::Debug for ObservingLogicalExtensionCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObservingLogicalExtensionCodec")
            .finish_non_exhaustive()
    }
}

impl LogicalExtensionCodec for ObservingLogicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        record_task_ctx(&self.observed, ctx);
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
        record_task_ctx(&self.observed, ctx);
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
}

/// Physical companion to [`ObservingLogicalExtensionCodec`].
struct ObservingPhysicalExtensionCodec {
    inner: DefaultPhysicalExtensionCodec,
    observed: ObservedMaxRows,
}

impl fmt::Debug for ObservingPhysicalExtensionCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObservingPhysicalExtensionCodec")
            .finish_non_exhaustive()
    }
}

impl PhysicalExtensionCodec for ObservingPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        record_task_ctx(&self.observed, ctx);
        self.inner.try_decode(buf, inputs, ctx, proto_converter)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        self.inner.try_encode(node, buf, proto_converter)
    }
}

fn task_ctx_provider_from_session(ctx: &Bound<'_, PyAny>) -> PyResult<FFI_TaskContextProvider> {
    let capsule = ctx.call_method0("__datafusion_task_context_provider__")?;
    let capsule = capsule.cast::<PyCapsule>()?;
    let provider: NonNull<FFI_TaskContextProvider> = capsule
        .pointer_checked(Some(c"datafusion_task_context_provider"))?
        .cast();
    // The FFI provider holds a weak reference; cloning it does not keep the
    // session context alive. The host's returned context is the strong owner.
    Ok(unsafe { provider.as_ref() }.clone())
}

/// Extension bundle for `SessionContext.with_extensions`.
///
/// Mirrors how a distributed engine such as Ballista packages its session
/// extensions: the object itself is reusable configuration, and every
/// `__datafusion_session_extension__` call creates fresh codec and planner
/// components bound to the task-context provider of the context it receives.
#[pyclass(
    from_py_object,
    name = "MyPlannerExtension",
    module = "datafusion_ffi_query_planner_example",
    subclass
)]
#[derive(Debug, Default, Clone)]
pub(crate) struct MyPlannerExtension {
    observations: Arc<PlannerObservations>,
    observed_max_rows: ObservedMaxRows,
}

#[pymethods]
impl MyPlannerExtension {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn plan_calls(&self) -> usize {
        self.observations.plan_calls.load(Ordering::SeqCst)
    }

    fn last_max_rows(&self) -> usize {
        self.observations.last_max_rows.load(Ordering::SeqCst)
    }

    fn foreign_session_observed(&self) -> bool {
        self.observations.foreign_session.load(Ordering::SeqCst)
    }

    fn foreign_provider_observed(&self) -> bool {
        self.observations.foreign_provider.load(Ordering::SeqCst)
    }

    fn foreign_plan_observed(&self) -> bool {
        self.observations.foreign_plan.load(Ordering::SeqCst)
    }

    /// `ffi_query_planner.max_rows` values seen through the bound
    /// task-context provider during codec decode calls.
    fn decode_max_rows_seen(&self) -> Vec<usize> {
        self.observed_max_rows
            .lock()
            .map(|observed| observed.clone())
            .unwrap_or_default()
    }

    fn __datafusion_session_extension__<'py>(
        &self,
        py: Python<'py>,
        ctx: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Bind every component to the destination context supplied by the
        // host. Components must not be cached across calls: each installation
        // targets a different context.
        let provider = task_ctx_provider_from_session(&ctx)?;
        let runtime = get_tokio_runtime().handle().clone();

        let logical: Arc<dyn LogicalExtensionCodec> = Arc::new(ObservingLogicalExtensionCodec {
            inner: DefaultLogicalExtensionCodec {},
            observed: Arc::clone(&self.observed_max_rows),
        });
        let ffi_logical =
            FFI_LogicalExtensionCodec::new(logical, Some(runtime.clone()), provider.clone());
        let logical_capsule =
            PyCapsule::new_with_value(py, ffi_logical, cr"datafusion_logical_extension_codec")?;

        let physical: Arc<dyn PhysicalExtensionCodec + Send> =
            Arc::new(ObservingPhysicalExtensionCodec {
                inner: DefaultPhysicalExtensionCodec {},
                observed: Arc::clone(&self.observed_max_rows),
            });
        let ffi_physical =
            FFI_PhysicalExtensionCodec::new(physical, Some(runtime.clone()), provider.clone());
        let physical_capsule =
            PyCapsule::new_with_value(py, ffi_physical, cr"datafusion_physical_extension_codec")?;

        let planner: Arc<dyn QueryPlanner + Send + Sync> = Arc::new(DistributedQueryPlanner {
            observations: Arc::clone(&self.observations),
        });
        let ffi_planner = FFI_QueryPlanner::new(
            planner,
            Some(runtime),
            provider,
            Arc::new(DefaultLogicalExtensionCodec {}),
            Arc::new(DefaultPhysicalExtensionCodec {}),
        );
        let planner_capsule =
            PyCapsule::new_with_value(py, ffi_planner, cr"datafusion_query_planner")?;

        let components = py
            .import("datafusion")?
            .getattr("SessionExtensionComponents")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("logical_extension_codecs", (logical_capsule,))?;
        kwargs.set_item("physical_extension_codecs", (physical_capsule,))?;
        kwargs.set_item("query_planner", planner_capsule)?;
        components.call((), Some(&kwargs))
    }
}
