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

use datafusion::logical_expr::Subquery;
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::sql::logical::PyLogicalPlan;

use super::logical_node::LogicalNode;

#[pyclass(name = "Subquery", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PySubquery {
    subquery: Subquery,
}

impl From<PySubquery> for Subquery {
    fn from(subquery: PySubquery) -> Self {
        subquery.subquery
    }
}

impl From<Subquery> for PySubquery {
    fn from(subquery: Subquery) -> PySubquery {
        PySubquery { subquery }
    }
}

impl Display for PySubquery {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Subquery
            Subquery: {:?}
            outer_ref_columns: {:?}",
            self.subquery.subquery, self.subquery.outer_ref_columns,
        )
    }
}

#[pymethods]
impl PySubquery {
    /// Retrieves the input `LogicalPlan` to this `Projection` node
    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Subquery({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("Subquery".to_string())
    }
}

impl LogicalNode for PySubquery {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
