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

use datafusion::physical_plan::{displayable, ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
use prost::Message;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::context::PySessionContext;
use crate::errors::PyDataFusionResult;

#[pyclass(frozen, name = "ExecutionPlan", module = "datafusion", subclass)]
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

    pub fn to_proto<'py>(&'py self, py: Python<'py>) -> PyDataFusionResult<Bound<'py, PyBytes>> {
        let codec = DefaultPhysicalExtensionCodec {};
        let proto = datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(
            self.plan.clone(),
            &codec,
        )?;

        let bytes = proto.encode_to_vec();
        Ok(PyBytes::new(py, &bytes))
    }

    #[staticmethod]
    pub fn from_proto(
        ctx: PySessionContext,
        proto_msg: Bound<'_, PyBytes>,
    ) -> PyDataFusionResult<Self> {
        let bytes: &[u8] = proto_msg.extract()?;
        let proto_plan =
            datafusion_proto::protobuf::PhysicalPlanNode::decode(bytes).map_err(|e| {
                PyRuntimeError::new_err(format!(
                    "Unable to decode logical node from serialized bytes: {e}"
                ))
            })?;

        let codec = DefaultPhysicalExtensionCodec {};
        let plan = proto_plan.try_into_physical_plan(ctx.ctx.task_ctx().as_ref(), &codec)?;
        Ok(Self::new(plan))
    }

    fn __repr__(&self) -> String {
        self.display_indent()
    }

    #[getter]
    pub fn partition_count(&self) -> usize {
        self.plan.output_partitioning().partition_count()
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
