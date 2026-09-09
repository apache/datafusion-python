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

//! This library's extension bundle: codecs only, no planner.
//!
//! A provider library has no business installing a query planner, so this
//! bundle implements `__datafusion_session_components__` and stops there.
//! `with_extensions` accepts a bundle that implements only one of the two
//! hooks.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_python_util::{
    create_physical_extension_capsule, ffi_task_context_provider_from_pycapsule, get_tokio_runtime,
};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict};

use crate::codec::{CodecCounters, DfxStoragePhysicalCodec};

/// Wire id this codec's payloads carry.
///
/// Pinned rather than left to default to the exporting class's import path,
/// because these payloads outlive the process that wrote them: a driver that
/// imports the class as `dfx_storage.BundledPhysicalCodec` and a worker that
/// imports it under any other name would otherwise disagree about the id and
/// every decode would fail.
const PHYSICAL_CODEC_ID: &str = "dfx_storage.physical.v1";

/// Carries this library's physical codec as an object rather than a capsule.
///
/// `with_extensions` requires an object: a codec's wire id is read off the
/// thing it is handed over as, and a capsule has no type to read one from.
#[pyclass(name = "BundledPhysicalCodec", module = "dfx_storage")]
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
/// Reusable configuration, not bound state: every
/// `__datafusion_session_components__` call builds fresh components against
/// the context it is handed, so one bundle may be installed on several
/// sessions.
#[pyclass(from_py_object, name = "DfxStorageExtension", module = "dfx_storage")]
#[derive(Default, Clone)]
pub(crate) struct DfxStorageExtension {
    counters: Arc<CodecCounters>,
}

impl fmt::Debug for DfxStorageExtension {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DfxStorageExtension")
            .field("counters", &self.counters)
            .finish_non_exhaustive()
    }
}

#[pymethods]
impl DfxStorageExtension {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    /// How often this codec encoded one of its own nodes.
    fn encode_calls(&self) -> usize {
        self.counters.encoded.load(Ordering::SeqCst)
    }

    /// How often it rebuilt one, which is the half that happens on a worker.
    fn decode_calls(&self) -> usize {
        self.counters.decoded.load(Ordering::SeqCst)
    }

    /// How often it was offered a node it does not own and passed it on.
    ///
    /// Non-zero is healthy: it means the chain is asking this codec about
    /// other libraries' nodes and it is declining them.
    fn declined_calls(&self) -> usize {
        self.counters.declined.load(Ordering::SeqCst)
    }

    /// The wire id, so a driver can put it in a worker's task envelope and
    /// the worker can check it before decoding anything.
    #[staticmethod]
    fn physical_codec_id() -> &'static str {
        PHYSICAL_CODEC_ID
    }

    fn __datafusion_session_components__<'py>(
        &self,
        py: Python<'py>,
        ctx: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Take the provider off the context supplied by the host, so the
        // codec's decode callbacks resolve against the session that will run
        // the query.
        let provider = ffi_task_context_provider_from_pycapsule(&ctx)?;
        let runtime = get_tokio_runtime().handle().clone();

        let codec: Arc<dyn PhysicalExtensionCodec + Send> =
            Arc::new(DfxStoragePhysicalCodec::new(Arc::clone(&self.counters)));
        let ffi = FFI_PhysicalExtensionCodec::new(codec, Some(runtime), provider);
        let physical = Py::new(py, BundledPhysicalCodec { codec: ffi })?;

        // No logical codec: this library defines no logical extension node.
        // Its table provider crosses FFI as a provider, not as a plan node.
        let components = py
            .import("datafusion")?
            .getattr("SessionExtensionComponents")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("physical_extension_codecs", (physical,))?;
        components.call((), Some(&kwargs))
    }
}
