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

use crate::errors::py_runtime_err;
use crate::expr::aggregate::PyAggregate;
use crate::expr::filter::PyFilter;
use crate::expr::limit::PyLimit;
use crate::expr::projection::PyProjection;
use crate::expr::sort::PySort;
use crate::expr::table_scan::PyTableScan;
use crate::expr::analyze::PyAnalyze;
use datafusion_expr::LogicalPlan;
use pyo3::prelude::*;

#[pyclass(name = "LogicalPlan", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyLogicalPlan {
    pub(crate) plan: Arc<LogicalPlan>,
}

impl PyLogicalPlan {
    /// creates a new PyLogicalPlan
    pub fn new(plan: LogicalPlan) -> Self {
        Self {
            plan: Arc::new(plan),
        }
    }

    pub fn plan(&self) -> Arc<LogicalPlan> {
        self.plan.clone()
    }
}

#[pymethods]
impl PyLogicalPlan {
    /// Return the specific logical operator
    fn to_variant(&self, py: Python) -> PyResult<PyObject> {
        Python::with_gil(|_| match self.plan.as_ref() {
            LogicalPlan::Aggregate(plan) => Ok(PyAggregate::from(plan.clone()).into_py(py)),
            LogicalPlan::Analyze(plan) => Ok(PyAnalyze::from(plan.clone()).into_py(py)),
            LogicalPlan::Filter(plan) => Ok(PyFilter::from(plan.clone()).into_py(py)),
            LogicalPlan::Limit(plan) => Ok(PyLimit::from(plan.clone()).into_py(py)),
            LogicalPlan::Projection(plan) => Ok(PyProjection::from(plan.clone()).into_py(py)),
            LogicalPlan::Sort(plan) => Ok(PySort::from(plan.clone()).into_py(py)),
            LogicalPlan::TableScan(plan) => Ok(PyTableScan::from(plan.clone()).into_py(py)),
            other => Err(py_runtime_err(format!(
                "Cannot convert this plan to a LogicalNode: {:?}",
                other
            ))),
        })
    }

    /// Get the inputs to this plan
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        let mut inputs = vec![];
        for input in self.plan.inputs() {
            inputs.push(input.to_owned().into());
        }
        inputs
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.plan))
    }

    fn display(&self) -> String {
        format!("{}", self.plan.display())
    }

    fn display_indent(&self) -> String {
        format!("{}", self.plan.display_indent())
    }

    fn display_indent_schema(&self) -> String {
        format!("{}", self.plan.display_indent_schema())
    }

    fn display_graphviz(&self) -> String {
        format!("{}", self.plan.display_indent_schema())
    }
}

impl From<PyLogicalPlan> for LogicalPlan {
    fn from(logical_plan: PyLogicalPlan) -> LogicalPlan {
        logical_plan.plan.as_ref().clone()
    }
}

impl From<LogicalPlan> for PyLogicalPlan {
    fn from(logical_plan: LogicalPlan) -> PyLogicalPlan {
        PyLogicalPlan {
            plan: Arc::new(logical_plan),
        }
    }
}
