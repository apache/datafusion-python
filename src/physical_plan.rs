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

use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

use pyo3::prelude::*;

#[pyclass(name = "ExecutionPlan", module = "substrait", subclass)]
#[derive(Debug, Clone)]
pub struct PyExecutionPlan {
    pub(crate) plan: Arc<dyn ExecutionPlan>,
}

impl PyExecutionPlan {
    /// creates a new PyPhysicalPlan
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { plan }
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
