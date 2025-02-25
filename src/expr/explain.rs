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

use std::fmt::{self, Display, Formatter};

use datafusion::logical_expr::{logical_plan::Explain, LogicalPlan};
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::{common::df_schema::PyDFSchema, errors::py_type_err, sql::logical::PyLogicalPlan};

use super::logical_node::LogicalNode;

#[pyclass(name = "Explain", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyExplain {
    explain: Explain,
}

impl From<PyExplain> for Explain {
    fn from(explain: PyExplain) -> Self {
        explain.explain
    }
}

impl From<Explain> for PyExplain {
    fn from(explain: Explain) -> PyExplain {
        PyExplain { explain }
    }
}

impl Display for PyExplain {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Explain
            verbose: {:?}
            plan: {:?}
            stringified_plans: {:?}
            schema: {:?}
            logical_optimization_succeeded: {:?}",
            &self.explain.verbose,
            &self.explain.plan,
            &self.explain.stringified_plans,
            &self.explain.schema,
            &self.explain.logical_optimization_succeeded
        )
    }
}

#[pymethods]
impl PyExplain {
    fn explain_string(&self) -> PyResult<Vec<String>> {
        let mut string_plans: Vec<String> = Vec::new();
        for stringified_plan in &self.explain.stringified_plans {
            string_plans.push((*stringified_plan.plan).clone());
        }
        Ok(string_plans)
    }

    fn verbose(&self) -> bool {
        self.explain.verbose
    }

    fn plan(&self) -> PyResult<PyLogicalPlan> {
        Ok(PyLogicalPlan::from((*self.explain.plan).clone()))
    }

    fn schema(&self) -> PyDFSchema {
        (*self.explain.schema).clone().into()
    }

    fn logical_optimization_succceeded(&self) -> bool {
        self.explain.logical_optimization_succeeded
    }
}

impl TryFrom<LogicalPlan> for PyExplain {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Explain(explain) => Ok(PyExplain { explain }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}

impl LogicalNode for PyExplain {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
