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

//! This library's extension bundle: codecs *and* a planner.
//!
//! Both hooks, because the two halves are useless apart. The planner emits a
//! node only this library's codec can carry, so installing the planner without
//! the codec produces plans that cannot be serialized -- and installing the
//! codec without the planner produces nothing for it to carry. Shipping them
//! as one object is what makes that impossible to get wrong, and it is why
//! `with_extensions` installs every bundle's codecs before it binds any
//! planner.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use datafusion_ffi::query_planner::FFI_QueryPlanner;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_python_util::{
    create_physical_extension_capsule, create_query_planner_capsule,
    ffi_logical_codec_from_pycapsule, ffi_physical_codec_from_pycapsule,
    ffi_query_planner_from_pycapsule, ffi_task_context_provider_from_pycapsule, get_tokio_runtime,
};
use datafusion_session::QueryPlanner;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict};

use crate::codec::{CodecCounters, DfxEnginePhysicalCodec};
use crate::planner::{DistributedQueryPlanner, PlannerObservations};

/// Wire id this codec's payloads carry, pinned because they cross processes.
const PHYSICAL_CODEC_ID: &str = "dfx_engine.physical.v1";

/// Carries this library's physical codec as an object rather than a capsule.
///
/// `with_extensions` requires an object: a codec's wire id is read off the
/// thing it is handed over as, and a capsule has no type to read one from.
/// Wrapping also keeps the id *this library's* -- an id derived from the
/// contributing bundle would follow whichever object the caller passed, so an
/// application packaging this engine inside a bundle of its own would silently
/// re-tag these payloads and they would stop decoding on the workers.
#[pyclass(name = "BundledPhysicalCodec", module = "dfx_engine")]
pub(crate) struct BundledPhysicalCodec {
    codec: FFI_PhysicalExtensionCodec,
}

#[pymethods]
impl BundledPhysicalCodec {
    #[getter]
    fn __datafusion_codec_id__(&self) -> &'static str {
        PHYSICAL_CODEC_ID
    }

    /// `session` is unused: the codec was bound to its task-context provider
    /// when the bundle was installed, which is why the bundle receives the
    /// context at all.
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
/// Reusable configuration, not bound state: components are built fresh against
/// whichever context each install hands over, so one bundle works on several
/// sessions.
#[pyclass(from_py_object, name = "DfxEngineExtension", module = "dfx_engine")]
#[derive(Default, Clone)]
pub(crate) struct DfxEngineExtension {
    observations: Arc<PlannerObservations>,
    counters: Arc<CodecCounters>,
}

impl fmt::Debug for DfxEngineExtension {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DfxEngineExtension")
            .field("observations", &self.observations)
            .finish_non_exhaustive()
    }
}

#[pymethods]
impl DfxEngineExtension {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    /// How often this engine's planner was asked for a physical plan.
    fn plan_calls(&self) -> usize {
        self.observations.plan_calls.load(Ordering::SeqCst)
    }

    /// How often it inserted a stage, which is the rewrite it exists to do.
    fn stages_inserted(&self) -> usize {
        self.observations.stages_inserted.load(Ordering::SeqCst)
    }

    /// How often this codec encoded one of its own stage nodes -- the step
    /// that happens when the driver ships work.
    fn encode_calls(&self) -> usize {
        self.counters.encoded.load(Ordering::SeqCst)
    }

    /// How often it rebuilt one, which happens on a worker.
    fn decode_calls(&self) -> usize {
        self.counters.decoded.load(Ordering::SeqCst)
    }

    /// The wire id, so a driver can put it in a worker's task envelope and the
    /// worker can check it before decoding anything.
    #[staticmethod]
    fn physical_codec_id() -> &'static str {
        PHYSICAL_CODEC_ID
    }

    fn __datafusion_session_components__<'py>(
        &self,
        py: Python<'py>,
        ctx: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Bind to the context the host supplied -- the session these
        // components will run on -- and build fresh ones every call. The
        // task-context provider comes off that context rather than from a
        // `SessionContext` built here, so decode callbacks resolve names
        // against the session that will actually run the query.
        let provider = ffi_task_context_provider_from_pycapsule(&ctx)?;
        let runtime = get_tokio_runtime().handle().clone();

        let codec: Arc<dyn PhysicalExtensionCodec + Send> =
            Arc::new(DfxEnginePhysicalCodec::new(Arc::clone(&self.counters)));
        let ffi = FFI_PhysicalExtensionCodec::new(codec, Some(runtime), provider);
        let physical = Py::new(py, BundledPhysicalCodec { codec: ffi })?;

        let components = py
            .import("datafusion")?
            .getattr("SessionExtensionComponents")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("physical_extension_codecs", (physical,))?;
        components.call((), Some(&kwargs))
    }

    /// Contribute this engine's planner, nesting it on whatever came before.
    ///
    /// Runs after every bundle's codecs are installed, so `ctx` carries the
    /// final chains and the planner is not left encoding through a partial
    /// set. `fallback` is the planner assembled so far; delegating to it is
    /// what makes several planner-shipping libraries composable, and
    /// returning a planner that ignored it would discard every layer beneath.
    fn __datafusion_session_planner__<'py>(
        &self,
        py: Python<'py>,
        ctx: Bound<'py, PyAny>,
        fallback: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let fallback = ffi_query_planner_from_pycapsule(&fallback, Some(&ctx))?;
        let planner: Arc<dyn QueryPlanner + Send + Sync> = Arc::new(DistributedQueryPlanner {
            observations: Arc::clone(&self.observations),
            // Deliberately not layered. Delegating would hand physical
            // planning to the host and bring the plan back as opaque foreign
            // nodes, which this engine cannot split -- so it plans for itself
            // and the fallback goes unused. A planner that only rearranged
            // stock nodes would keep it.
            fallback: None,
        });
        let _ = fallback;

        // The planner takes the *host's* codecs, not ones built here. By now
        // those are the final chains, and this library has no business
        // minting a task-context provider of its own.
        let host_logical = ffi_logical_codec_from_pycapsule(ctx.clone(), None)?;
        let host_physical = ffi_physical_codec_from_pycapsule(ctx, None)?;
        let ffi_planner =
            FFI_QueryPlanner::new_with_ffi_codecs(planner, host_logical, host_physical);
        create_query_planner_capsule(py, &ffi_planner)
    }
}
