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

use datafusion::logical_expr::logical_plan::Unnest;
use pyo3::{prelude::*, IntoPyObjectExt};
use std::fmt::{self, Display, Formatter};

use crate::common::df_schema::PyDFSchema;
use crate::expr::logical_node::LogicalNode;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "Unnest", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyUnnest {
    unnest_: Unnest,
}

impl From<Unnest> for PyUnnest {
    fn from(unnest_: Unnest) -> PyUnnest {
        PyUnnest { unnest_ }
    }
}

impl From<PyUnnest> for Unnest {
    fn from(unnest_: PyUnnest) -> Self {
        unnest_.unnest_
    }
}

impl Display for PyUnnest {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Unnest
            Inputs: {:?}
            Schema: {:?}",
            &self.unnest_.input, &self.unnest_.schema,
        )
    }
}

#[pymethods]
impl PyUnnest {
    /// Retrieves the input `LogicalPlan` to this `Unnest` node
    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    /// Resulting Schema for this `Unnest` node instance
    fn schema(&self) -> PyResult<PyDFSchema> {
        Ok(self.unnest_.schema.as_ref().clone().into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Unnest({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("Unnest".to_string())
    }
}

impl LogicalNode for PyUnnest {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.unnest_.input).clone())]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
