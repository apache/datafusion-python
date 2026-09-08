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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::common::{Result, internal_err};
use datafusion::execution::TaskContext;
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
    ffi_physical_codec_from_pycapsule, ffi_query_planner_from_pycapsule,
    ffi_task_context_provider_from_pycapsule, get_tokio_runtime,
};
use datafusion_session::QueryPlanner;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict};

use crate::distributed_exec::DistributedExec;
use crate::planner::{DistributedQueryPlanner, PlannerObservations, planner_config_from_options};

/// Values of `ffi_query_planner.max_rows` observed through the task-context
/// provider bound at installation time.
///
/// Recorded on every decode call the chain routes to this bundle's physical
/// codec, including ones it declines. Reaching the session config from inside
/// a decode callback is what proves the provider bound at installation
/// resolves against the session running the query.
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

/// Carries this library's own [`DistributedExec`] nodes.
///
/// This is the codec half of the bundle, and the reason the bundle ships both.
/// `DistributedQueryPlanner` emits a `DistributedExec`; no other codec in the
/// session knows the type, so without this one the plans that planner produces
/// cannot be serialized at all. Anything else is declined by delegating to the
/// default codec, so the host's chain falls through to whichever library owns
/// the node.
///
/// The payload is a marker rather than a serialized node. `DistributedExec` is
/// pass-through and its child arrives already decoded in `inputs`, so there is
/// nothing else to write down; a node with state of its own would encode that
/// state here.
struct ObservingPhysicalExtensionCodec {
    inner: DefaultPhysicalExtensionCodec,
    observed: ObservedMaxRows,
    claims: Arc<DistributedExecClaims>,
}

/// How often this bundle's codec claimed one of its own nodes.
///
/// Distinct from [`ObservedMaxRows`], which counts every call the chain made,
/// including ones this codec declined.
#[derive(Default, Debug)]
pub(crate) struct DistributedExecClaims {
    encoded: AtomicUsize,
    decoded: AtomicUsize,
}

/// Payload written for a [`DistributedExec`]. See
/// [`ObservingPhysicalExtensionCodec`].
const DISTRIBUTED_EXEC_MARKER: &[u8] = b"datafusion_ffi_query_planner_example:DistributedExec";

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
        // Reading the config through `ctx` is what proves the task-context
        // provider bound at installation resolves against the session running
        // the query. It happens on the real decode path now, not a synthetic
        // one.
        record_task_ctx(&self.observed, ctx);
        if buf == DISTRIBUTED_EXEC_MARKER {
            let [input] = inputs else {
                return internal_err!(
                    "DistributedExec expects exactly one input, got {}",
                    inputs.len()
                );
            };
            self.claims.decoded.fetch_add(1, Ordering::SeqCst);
            return Ok(Arc::new(DistributedExec::new(Arc::clone(input))));
        }
        self.inner.try_decode(buf, inputs, ctx, proto_converter)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        if node.is::<DistributedExec>() {
            self.claims.encoded.fetch_add(1, Ordering::SeqCst);
            buf.extend_from_slice(DISTRIBUTED_EXEC_MARKER);
            return Ok(());
        }
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
    claims: Arc<DistributedExecClaims>,
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
    /// One entry per decode call the chain routed to this bundle, so a query
    /// whose plan carries a `DistributedExec` leaves several.
    fn decode_max_rows_seen(&self) -> Vec<usize> {
        self.observed_max_rows
            .lock()
            .map(|observed| observed.clone())
            .unwrap_or_default()
    }

    /// How often this bundle's physical codec encoded one of the
    /// `DistributedExec` nodes its own planner produced.
    ///
    /// Non-zero only when the plan was actually serialized — running a query
    /// does not do that, because an FFI planner hands its result back as an
    /// opaque plan handle. A distributed engine shipping the plan to a remote
    /// executor does, which is the case the pairing exists for; in this
    /// repository `ExecutionPlan.to_bytes` stands in for it.
    fn distributed_exec_encode_calls(&self) -> usize {
        self.claims.encoded.load(Ordering::SeqCst)
    }

    /// Companion to [`Self::distributed_exec_encode_calls`], counting the
    /// nodes rebuilt on the way back in.
    fn distributed_exec_decode_calls(&self) -> usize {
        self.claims.decoded.load(Ordering::SeqCst)
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

        // Plain default: this library defines no logical extension node, so
        // there is nothing for a logical codec of its own to claim. It is still
        // contributed so the bundle carries both codec kinds under ids it
        // declares -- see `BundledLogicalCodec`.
        let logical: Arc<dyn LogicalExtensionCodec> = Arc::new(DefaultLogicalExtensionCodec {});
        let ffi_logical =
            FFI_LogicalExtensionCodec::new(logical, Some(runtime.clone()), provider.clone());
        // Handed over as an object, not a capsule, so the codec carries an id
        // of its own. See `BundledLogicalCodec`.
        let logical_codec = Py::new(py, BundledLogicalCodec { codec: ffi_logical })?;

        let physical: Arc<dyn PhysicalExtensionCodec + Send> =
            Arc::new(ObservingPhysicalExtensionCodec {
                inner: DefaultPhysicalExtensionCodec {},
                observed: Arc::clone(&self.observed_max_rows),
                claims: Arc::clone(&self.claims),
            });
        let ffi_physical =
            FFI_PhysicalExtensionCodec::new(physical, Some(runtime), provider.clone());
        let physical_codec = Py::new(
            py,
            BundledPhysicalCodec {
                codec: ffi_physical,
            },
        )?;

        let components = py
            .import("datafusion")?
            .getattr("SessionExtensionComponents")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("logical_extension_codecs", (logical_codec,))?;
        kwargs.set_item("physical_extension_codecs", (physical_codec,))?;
        components.call((), Some(&kwargs))
    }

    /// Contribute this library's planner, nesting it on whatever came before.
    ///
    /// Runs in the host's second phase, after every bundle's codecs are
    /// installed, so `ctx` carries the final chains and the planner this
    /// builds is not left encoding through a partial set. `fallback` is the
    /// planner assembled so far — the session's existing one for the first
    /// bundle, the previous bundle's for the rest — and delegating to it is
    /// what makes several planner-shipping libraries composable. Returning a
    /// planner that ignored it would discard every layer beneath.
    fn __datafusion_session_planner__<'py>(
        &self,
        py: Python<'py>,
        ctx: Bound<'py, PyAny>,
        fallback: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let fallback = ffi_query_planner_from_pycapsule(&fallback, Some(&ctx))?;
        let planner: Arc<dyn QueryPlanner + Send + Sync> = Arc::new(DistributedQueryPlanner {
            observations: Arc::clone(&self.observations),
            fallback: Some((&fallback).into()),
        });
        // The planner takes the host's codecs, not ones built here. By now
        // those are the final chains, and this library has no business minting
        // a provider of its own.
        let host_logical = ffi_logical_codec_from_pycapsule(ctx.clone(), None)?;
        let host_physical = ffi_physical_codec_from_pycapsule(ctx, None)?;
        let ffi_planner =
            FFI_QueryPlanner::new_with_ffi_codecs(planner, host_logical, host_physical);
        create_query_planner_capsule(py, &ffi_planner)
    }
}
