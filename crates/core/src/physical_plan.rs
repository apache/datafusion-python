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

use arrow::datatypes::Schema;
use arrow::pyarrow::FromPyArrow;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};
use datafusion_ffi::execution_plan::FFI_ExecutionPlan;
use datafusion_ffi::physical_expr::FFI_PhysicalExpr;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::protobuf;
use datafusion_python_util::get_tokio_runtime;
use prost::Message;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule};

use crate::codec::PythonPhysicalCodec;
use crate::context::PySessionContext;
use crate::errors::{PyDataFusionResult, py_datafusion_err};
use crate::metrics::PyMetricsSet;

#[pyclass(
    from_py_object,
    frozen,
    name = "ExecutionPlan",
    module = "datafusion",
    subclass
)]
#[derive(Debug, Clone)]
pub struct PyExecutionPlan {
    pub plan: Arc<dyn ExecutionPlan>,
}

impl PyExecutionPlan {
    /// creates a new PyPhysicalPlan
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { plan }
    }
}

#[pymethods]
impl PyExecutionPlan {
    /// Get the inputs to this plan
    pub fn children(&self) -> Vec<PyExecutionPlan> {
        self.plan
            .children()
            .iter()
            .map(|&p| p.to_owned().into())
            .collect()
    }

    pub fn display(&self) -> String {
        let d = displayable(self.plan.as_ref());
        format!("{}", d.one_line())
    }

    pub fn display_indent(&self) -> String {
        let d = displayable(self.plan.as_ref());
        format!("{}", d.indent(false))
    }

    #[pyo3(signature = (ctx=None))]
    pub fn to_bytes<'py>(
        &'py self,
        py: Python<'py>,
        ctx: Option<PySessionContext>,
    ) -> PyDataFusionResult<Bound<'py, PyBytes>> {
        // Route through the session's physical codec when supplied so
        // user FFI codecs registered via
        // `with_physical_extension_codec` see the encode path.
        let default_codec;
        let codec: &dyn datafusion_proto::physical_plan::PhysicalExtensionCodec = match ctx {
            Some(ref ctx) => ctx.physical_codec().as_ref(),
            None => {
                default_codec = PythonPhysicalCodec::default();
                &default_codec
            }
        };
        let proto = datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(
            self.plan.clone(),
            codec,
        )?;

        let bytes = proto.encode_to_vec();
        Ok(PyBytes::new(py, &bytes))
    }

    #[staticmethod]
    pub fn from_bytes(
        ctx: PySessionContext,
        proto_msg: Bound<'_, PyBytes>,
    ) -> PyDataFusionResult<Self> {
        let bytes: &[u8] = proto_msg.extract().map_err(Into::<PyErr>::into)?;
        let proto_plan =
            datafusion_proto::protobuf::PhysicalPlanNode::decode(bytes).map_err(|e| {
                PyRuntimeError::new_err(format!(
                    "Unable to decode physical node from serialized bytes: {e}"
                ))
            })?;

        let codec = ctx.physical_codec();
        let plan =
            proto_plan.try_into_physical_plan(ctx.ctx.task_ctx().as_ref(), codec.as_ref())?;
        Ok(Self::new(plan))
    }

    /// Deprecated alias for [`Self::to_bytes`]. Will be removed in a
    /// future release.
    pub fn to_proto<'py>(&'py self, py: Python<'py>) -> PyDataFusionResult<Bound<'py, PyBytes>> {
        emit_deprecation(
            py,
            "PyExecutionPlan.to_proto is deprecated; use to_bytes instead",
        )?;
        self.to_bytes(py, None)
    }

    /// Deprecated alias for [`Self::from_bytes`]. Will be removed in a
    /// future release.
    #[staticmethod]
    pub fn from_proto(
        py: Python<'_>,
        ctx: PySessionContext,
        proto_msg: Bound<'_, PyBytes>,
    ) -> PyDataFusionResult<Self> {
        emit_deprecation(
            py,
            "PyExecutionPlan.from_proto is deprecated; use from_bytes instead",
        )?;
        Self::from_bytes(ctx, proto_msg)
    }

    pub fn metrics(&self) -> Option<PyMetricsSet> {
        self.plan.metrics().map(PyMetricsSet::new)
    }

    fn __repr__(&self) -> String {
        self.display_indent()
    }

    #[getter]
    pub fn partition_count(&self) -> usize {
        self.plan.output_partitioning().partition_count()
    }

    /// Extract a `PyExecutionPlan` from any object exposing
    /// `__datafusion_execution_plan__()` (PyCapsule protocol).
    #[staticmethod]
    pub fn from_pycapsule(mut capsule: Bound<PyAny>) -> PyResult<Self> {
        if capsule.hasattr("__datafusion_execution_plan__")? {
            capsule = capsule.getattr("__datafusion_execution_plan__")?.call0()?;
        }

        let capsule = capsule.cast::<PyCapsule>().map_err(py_datafusion_err)?;
        let data: std::ptr::NonNull<FFI_ExecutionPlan> = capsule
            .pointer_checked(Some(c"datafusion_execution_plan"))?
            .cast();
        let plan = unsafe { data.as_ref() };
        let plan: Arc<dyn ExecutionPlan> = plan.try_into().map_err(py_datafusion_err)?;

        Ok(Self { plan })
    }

    pub fn __datafusion_execution_plan__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_execution_plan".into();
        let runtime = get_tokio_runtime().handle().clone();
        let plan = FFI_ExecutionPlan::new(self.plan.clone(), Some(runtime));
        PyCapsule::new(py, plan, Some(name))
    }
}

impl From<PyExecutionPlan> for Arc<dyn ExecutionPlan> {
    fn from(plan: PyExecutionPlan) -> Arc<dyn ExecutionPlan> {
        plan.plan.clone()
    }
}

impl From<Arc<dyn ExecutionPlan>> for PyExecutionPlan {
    fn from(plan: Arc<dyn ExecutionPlan>) -> PyExecutionPlan {
        PyExecutionPlan { plan: plan.clone() }
    }
}

/// Thin Python wrapper around a `PhysicalExpr` exposing serialization
/// + PyCapsule protocol. Mirrors [`PyExecutionPlan`] shape.
#[pyclass(
    from_py_object,
    frozen,
    name = "PhysicalExpr",
    module = "datafusion",
    subclass
)]
#[derive(Debug, Clone)]
pub struct PyPhysicalExpr {
    pub expr: Arc<dyn PhysicalExpr>,
}

impl PyPhysicalExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

#[pymethods]
impl PyPhysicalExpr {
    fn __repr__(&self) -> String {
        format!("{}", self.expr)
    }

    #[pyo3(signature = (ctx=None))]
    pub fn to_bytes<'py>(
        &self,
        py: Python<'py>,
        ctx: Option<PySessionContext>,
    ) -> PyDataFusionResult<Bound<'py, PyBytes>> {
        let default_codec;
        let codec: &dyn datafusion_proto::physical_plan::PhysicalExtensionCodec = match ctx {
            Some(ref ctx) => ctx.physical_codec().as_ref(),
            None => {
                default_codec = PythonPhysicalCodec::default();
                &default_codec
            }
        };
        let proto = serialize_physical_expr(&self.expr, codec)?;
        let bytes = proto.encode_to_vec();
        Ok(PyBytes::new(py, &bytes))
    }

    /// Decode a `PhysicalExpr` from serialized protobuf bytes. The
    /// receiver must supply the `input_schema` against which column
    /// references in the expression resolve (pyarrow Schema).
    #[staticmethod]
    pub fn from_bytes(
        ctx: PySessionContext,
        proto_msg: Bound<'_, PyBytes>,
        input_schema: Bound<'_, PyAny>,
    ) -> PyDataFusionResult<Self> {
        let bytes: &[u8] = proto_msg.extract().map_err(Into::<PyErr>::into)?;
        let proto_expr = protobuf::PhysicalExprNode::decode(bytes).map_err(|e| {
            PyRuntimeError::new_err(format!(
                "Unable to decode physical expression from serialized bytes: {e}"
            ))
        })?;
        let schema = Schema::from_pyarrow_bound(&input_schema)?;
        let codec = ctx.physical_codec();
        let task_ctx = ctx.ctx.task_ctx();
        let expr = parse_physical_expr(&proto_expr, task_ctx.as_ref(), &schema, codec.as_ref())?;
        Ok(Self::new(expr))
    }

    #[staticmethod]
    pub fn from_pycapsule(mut capsule: Bound<PyAny>) -> PyResult<Self> {
        if capsule.hasattr("__datafusion_physical_expr__")? {
            capsule = capsule.getattr("__datafusion_physical_expr__")?.call0()?;
        }

        let capsule = capsule.cast::<PyCapsule>().map_err(py_datafusion_err)?;
        let data: std::ptr::NonNull<FFI_PhysicalExpr> = capsule
            .pointer_checked(Some(c"datafusion_physical_expr"))?
            .cast();
        let ffi = unsafe { data.as_ref() };
        let expr: Arc<dyn PhysicalExpr> = ffi.into();
        Ok(Self { expr })
    }

    pub fn __datafusion_physical_expr__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_physical_expr".into();
        let ffi = FFI_PhysicalExpr::from(self.expr.clone());
        PyCapsule::new(py, ffi, Some(name))
    }
}

impl From<PyPhysicalExpr> for Arc<dyn PhysicalExpr> {
    fn from(expr: PyPhysicalExpr) -> Arc<dyn PhysicalExpr> {
        expr.expr.clone()
    }
}

impl From<Arc<dyn PhysicalExpr>> for PyPhysicalExpr {
    fn from(expr: Arc<dyn PhysicalExpr>) -> PyPhysicalExpr {
        PyPhysicalExpr { expr }
    }
}

fn emit_deprecation(py: Python<'_>, msg: &str) -> PyResult<()> {
    let warnings = py.import("warnings")?;
    let category = py.import("builtins")?.getattr("DeprecationWarning")?;
    warnings.call_method1("warn", (msg, category, 2))?;
    Ok(())
}
