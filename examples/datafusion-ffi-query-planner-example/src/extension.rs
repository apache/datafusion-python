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
use datafusion_python_util::{
    create_logical_extension_capsule, create_physical_extension_capsule,
    create_query_planner_capsule, ffi_logical_codec_from_pycapsule,
    ffi_physical_codec_from_pycapsule, ffi_task_context_provider_from_pycapsule, get_tokio_runtime,
};
use datafusion_session::QueryPlanner;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict};

use crate::planner::{DistributedQueryPlanner, PlannerObservations, planner_config_from_options};

/// Values of `ffi_query_planner.max_rows` observed through the task-context
/// provider bound at installation time.
///
/// Only populated when a codec in this bundle is actually consulted. The host
/// dispatches a framed payload straight to the codec whose id it names, so a
/// decline-all codec like the ones here is normally never asked to decode. The
/// binding itself is proved by [`MyPlannerExtension::max_rows_through_provider`],
/// which reads the provider directly rather than waiting for a callback.
type ObservedMaxRows = Arc<Mutex<Vec<usize>>>;

/// The task-context provider handed to this bundle's components, if it has been
/// installed. `FFI_TaskContextProvider` holds its session weakly, so keeping one
/// here does not keep that session alive.
type BoundProvider = Arc<Mutex<Option<FFI_TaskContextProvider>>>;

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

/// Wire id this library's logical codec claims, pinned so that renaming the
/// Rust or Python types does not invalidate plans already encoded.
const LOGICAL_CODEC_ID: &str = "datafusion_ffi_query_planner_example.logical.v1";

/// Physical companion to [`LOGICAL_CODEC_ID`].
const PHYSICAL_CODEC_ID: &str = "datafusion_ffi_query_planner_example.physical.v1";

/// Carries this bundle's logical codec as an object rather than a bare capsule.
///
/// `with_extensions` requires an object: a codec's wire id is read off the
/// thing it is handed over as, and a capsule has no type to read one from.
/// Wrapping is also what keeps the id *this library's*. An id derived from the
/// contributing bundle would follow whichever object the caller passed to
/// `with_extensions`, so an application that packages this library inside a
/// bundle of its own would silently re-tag these payloads and they would stop
/// decoding in the process that reads them. The wrapper travels with the codec;
/// the bundle does not.
///
/// Declaring `__datafusion_codec_id__` is optional — the class's
/// `module.QualName` would serve — but a library whose plans leave the process
/// should pin the id rather than let a refactor move it.
#[pyclass(
    name = "BundledLogicalCodec",
    module = "datafusion_ffi_query_planner_example"
)]
pub(crate) struct BundledLogicalCodec {
    codec: FFI_LogicalExtensionCodec,
}

#[pymethods]
impl BundledLogicalCodec {
    #[getter]
    fn __datafusion_codec_id__(&self) -> &'static str {
        LOGICAL_CODEC_ID
    }

    /// `session` is unused: the codec was bound to its task-context provider
    /// when the bundle was installed, which is the whole reason the bundle
    /// receives the context.
    #[pyo3(signature = (session=None))]
    fn __datafusion_logical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
        session: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = session;
        create_logical_extension_capsule(py, &self.codec)
    }
}

/// Physical companion to [`BundledLogicalCodec`].
#[pyclass(
    name = "BundledPhysicalCodec",
    module = "datafusion_ffi_query_planner_example"
)]
pub(crate) struct BundledPhysicalCodec {
    codec: FFI_PhysicalExtensionCodec,
}

#[pymethods]
impl BundledPhysicalCodec {
    #[getter]
    fn __datafusion_codec_id__(&self) -> &'static str {
        PHYSICAL_CODEC_ID
    }

    /// See [`BundledLogicalCodec::__datafusion_logical_extension_codec__`].
    #[pyo3(signature = (session=None))]
    fn __datafusion_physical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
        session: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = session;
        create_physical_extension_capsule(py, &self.codec)
    }
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
#[derive(Default, Clone)]
pub(crate) struct MyPlannerExtension {
    observations: Arc<PlannerObservations>,
    observed_max_rows: ObservedMaxRows,
    bound_provider: BoundProvider,
}

impl fmt::Debug for MyPlannerExtension {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MyPlannerExtension")
            .field("observations", &self.observations)
            .finish_non_exhaustive()
    }
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
    ///
    /// Usually empty: the host routes a framed payload to the codec named in
    /// it, so codecs that own nothing are not consulted.
    fn decode_max_rows_seen(&self) -> Vec<usize> {
        self.observed_max_rows
            .lock()
            .map(|observed| observed.clone())
            .unwrap_or_default()
    }

    /// `ffi_query_planner.max_rows` read through the task-context provider
    /// this bundle was last bound to.
    ///
    /// Resolving the provider is what a codec's decode callback does, so this
    /// answers which session those callbacks would resolve against -- the
    /// context `with_extensions` returned, not the one it was called on.
    /// Returns ``None`` if the bundle was never installed, or if the context it
    /// was bound to has been dropped: the provider holds it weakly.
    fn max_rows_through_provider(&self) -> Option<usize> {
        let provider = self.bound_provider.lock().ok()?.clone()?;
        let task_ctx = Arc::<TaskContext>::try_from(&provider).ok()?;
        planner_config_from_options(task_ctx.session_config().options())
            .ok()
            .map(|config| config.max_rows)
    }

    fn __datafusion_session_extension__<'py>(
        &self,
        py: Python<'py>,
        ctx: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Bind every component to the context supplied by the host, which is
        // the session the components will run on. Components must not be
        // cached across calls: each installation may target a different
        // session.
        //
        // The task-context provider comes off that context rather than from a
        // `SessionContext` built here, so the codecs' decode callbacks resolve
        // names against the session that will actually run the query.
        let provider = ffi_task_context_provider_from_pycapsule(&ctx)?;
        if let Ok(mut bound) = self.bound_provider.lock() {
            *bound = Some(provider.clone());
        }
        let runtime = get_tokio_runtime().handle().clone();

        let logical: Arc<dyn LogicalExtensionCodec> = Arc::new(ObservingLogicalExtensionCodec {
            inner: DefaultLogicalExtensionCodec {},
            observed: Arc::clone(&self.observed_max_rows),
        });
        let ffi_logical =
            FFI_LogicalExtensionCodec::new(logical, Some(runtime.clone()), provider.clone());
        // Handed over as an object, not a capsule, so the codec carries an id
        // of its own. See `BundledLogicalCodec`.
        let logical_codec = Py::new(py, BundledLogicalCodec { codec: ffi_logical })?;

        let physical: Arc<dyn PhysicalExtensionCodec + Send> =
            Arc::new(ObservingPhysicalExtensionCodec {
                inner: DefaultPhysicalExtensionCodec {},
                observed: Arc::clone(&self.observed_max_rows),
            });
        let ffi_physical =
            FFI_PhysicalExtensionCodec::new(physical, Some(runtime), provider.clone());
        let physical_codec = Py::new(
            py,
            BundledPhysicalCodec {
                codec: ffi_physical,
            },
        )?;

        let planner: Arc<dyn QueryPlanner + Send + Sync> = Arc::new(DistributedQueryPlanner {
            observations: Arc::clone(&self.observations),
            fallback: None,
        });
        // The planner takes the host's codecs, not ones built here. Installing
        // the codecs above rebuilds the planner against them anyway, and this
        // library has no business minting a provider of its own.
        let host_logical = ffi_logical_codec_from_pycapsule(ctx.clone(), None)?;
        let host_physical = ffi_physical_codec_from_pycapsule(ctx, None)?;
        let ffi_planner =
            FFI_QueryPlanner::new_with_ffi_codecs(planner, host_logical, host_physical);
        let planner_capsule = create_query_planner_capsule(py, &ffi_planner)?;

        let components = py
            .import("datafusion")?
            .getattr("SessionExtensionComponents")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("logical_extension_codecs", (logical_codec,))?;
        kwargs.set_item("physical_extension_codecs", (physical_codec,))?;
        kwargs.set_item("query_planner", planner_capsule)?;
        components.call((), Some(&kwargs))
    }
}
