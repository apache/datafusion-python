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

//! The Python surface, and the one thing it deliberately lacks.
//!
//! This library exposes **no** `__datafusion_session_components__`, so it
//! cannot be installed with `SessionContext.with_extensions`. Callers register
//! the three functions and install the two codecs by hand, which is the older
//! and more error-prone path -- and currently the honest one for a function
//! library, because `SessionExtensionComponents` carries codec fields only.
//! There is nowhere for a UDF to go.
//!
//! Keeping one library on the manual path is the point: a real deployment
//! mixes libraries built against different versions of the protocol, and the
//! example should show what that costs rather than pretend every dependency
//! has caught up.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use datafusion_ffi::udaf::FFI_AggregateUDF;
use datafusion_ffi::udf::FFI_ScalarUDF;
use datafusion_ffi::udwf::FFI_WindowUDF;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_python_util::{
    create_logical_extension_capsule, create_physical_extension_capsule,
    ffi_task_context_provider_from_pycapsule, get_tokio_runtime,
};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::codec::{CodecCounters, DfxUdfsLogicalCodec, DfxUdfsPhysicalCodec};
use crate::functions::{NetRevenue, RevenueRank, WeightedAvg};

/// Wire ids, pinned so a rename of the exporting class cannot invalidate
/// plans already written.
const LOGICAL_CODEC_ID: &str = "dfx_udfs.logical.v1";
const PHYSICAL_CODEC_ID: &str = "dfx_udfs.physical.v1";

/// `dfx_net_revenue(extendedprice, discount, tax)`.
#[pyclass(name = "NetRevenueUDF", module = "dfx_udfs")]
#[derive(Default)]
pub(crate) struct PyNetRevenue;

#[pymethods]
impl PyNetRevenue {
    #[new]
    fn new() -> Self {
        Self
    }

    fn __datafusion_scalar_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let func = Arc::new(ScalarUDF::from(NetRevenue::default()));
        PyCapsule::new_with_value(py, FFI_ScalarUDF::from(func), cr"datafusion_scalar_udf")
    }
}

/// `dfx_weighted_avg(value, weight)`.
#[pyclass(name = "WeightedAvgUDAF", module = "dfx_udfs")]
#[derive(Default)]
pub(crate) struct PyWeightedAvg;

#[pymethods]
impl PyWeightedAvg {
    #[new]
    fn new() -> Self {
        Self
    }

    fn __datafusion_aggregate_udf__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let func = Arc::new(AggregateUDF::from(WeightedAvg::default()));
        PyCapsule::new_with_value(
            py,
            FFI_AggregateUDF::from(func),
            cr"datafusion_aggregate_udf",
        )
    }
}

/// `dfx_revenue_rank()`, as a window function.
#[pyclass(name = "RevenueRankUDWF", module = "dfx_udfs")]
#[derive(Default)]
pub(crate) struct PyRevenueRank;

#[pymethods]
impl PyRevenueRank {
    #[new]
    fn new() -> Self {
        Self
    }

    fn __datafusion_window_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let func = Arc::new(WindowUDF::from(RevenueRank::default()));
        PyCapsule::new_with_value(py, FFI_WindowUDF::from(func), cr"datafusion_window_udf")
    }
}

/// Shared decode counters, so a test can see which codec answered.
#[pyclass(from_py_object, name = "CodecObservations", module = "dfx_udfs")]
#[derive(Default, Clone)]
pub(crate) struct PyCodecObservations {
    counters: Arc<CodecCounters>,
}

#[pymethods]
impl PyCodecObservations {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    /// How often a codec rebuilt one of this library's functions from its name.
    ///
    /// Zero after a successful query on a session that registered the
    /// functions: the registry is tried first, so the codec is only reached
    /// when the receiving session does *not* have them.
    fn decode_calls(&self) -> usize {
        self.counters.decoded.load(Ordering::SeqCst)
    }

    /// How often a codec was asked about a name it does not own.
    ///
    /// Non-zero is expected. A name-only payload has no codec id to route on,
    /// so it is offered to every installed codec in turn.
    fn declined_calls(&self) -> usize {
        self.counters.declined.load(Ordering::SeqCst)
    }

    /// Build the logical codec, sharing these counters.
    fn logical_codec(&self) -> PyLogicalCodec {
        PyLogicalCodec {
            counters: Arc::clone(&self.counters),
        }
    }

    /// Build the physical codec, sharing these counters.
    fn physical_codec(&self) -> PyPhysicalCodec {
        PyPhysicalCodec {
            counters: Arc::clone(&self.counters),
        }
    }
}

/// Install with `ctx.with_logical_extension_codec(...)`.
#[pyclass(name = "DfxUdfsLogicalCodec", module = "dfx_udfs")]
pub(crate) struct PyLogicalCodec {
    counters: Arc<CodecCounters>,
}

#[pymethods]
impl PyLogicalCodec {
    #[new]
    fn new() -> Self {
        Self {
            counters: Arc::default(),
        }
    }

    #[getter]
    fn __datafusion_codec_id__(&self) -> &'static str {
        LOGICAL_CODEC_ID
    }

    fn __datafusion_logical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let provider = ffi_task_context_provider_from_pycapsule(&session)?;
        let runtime = get_tokio_runtime().handle().clone();
        let codec: Arc<dyn LogicalExtensionCodec> =
            Arc::new(DfxUdfsLogicalCodec::new(Arc::clone(&self.counters)));
        let ffi = FFI_LogicalExtensionCodec::new(codec, Some(runtime), provider);
        create_logical_extension_capsule(py, &ffi)
    }
}

/// Install with `ctx.with_physical_extension_codec(...)`.
#[pyclass(name = "DfxUdfsPhysicalCodec", module = "dfx_udfs")]
pub(crate) struct PyPhysicalCodec {
    counters: Arc<CodecCounters>,
}

#[pymethods]
impl PyPhysicalCodec {
    #[new]
    fn new() -> Self {
        Self {
            counters: Arc::default(),
        }
    }

    #[getter]
    fn __datafusion_codec_id__(&self) -> &'static str {
        PHYSICAL_CODEC_ID
    }

    fn __datafusion_physical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let provider = ffi_task_context_provider_from_pycapsule(&session)?;
        let runtime = get_tokio_runtime().handle().clone();
        let codec: Arc<dyn PhysicalExtensionCodec + Send> =
            Arc::new(DfxUdfsPhysicalCodec::new(Arc::clone(&self.counters)));
        let ffi = FFI_PhysicalExtensionCodec::new(codec, Some(runtime), provider);
        create_physical_extension_capsule(py, &ffi)
    }
}
