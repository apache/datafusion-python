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

use datafusion::logical_expr::RecursiveQuery;
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::sql::logical::PyLogicalPlan;

use super::logical_node::LogicalNode;

#[pyclass(name = "RecursiveQuery", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyRecursiveQuery {
    query: RecursiveQuery,
}

impl From<PyRecursiveQuery> for RecursiveQuery {
    fn from(query: PyRecursiveQuery) -> Self {
        query.query
    }
}

impl From<RecursiveQuery> for PyRecursiveQuery {
    fn from(query: RecursiveQuery) -> PyRecursiveQuery {
        PyRecursiveQuery { query }
    }
}

impl Display for PyRecursiveQuery {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "RecursiveQuery {name:?} is_distinct:={is_distinct}",
            name = self.query.name,
            is_distinct = self.query.is_distinct
        )
    }
}

#[pymethods]
impl PyRecursiveQuery {
    #[new]
    fn new(
        name: String,
        static_term: PyLogicalPlan,
        recursive_term: PyLogicalPlan,
        is_distinct: bool,
    ) -> Self {
        Self {
            query: RecursiveQuery {
                name,
                static_term: static_term.plan(),
                recursive_term: recursive_term.plan(),
                is_distinct,
            },
        }
    }

    fn name(&self) -> PyResult<String> {
        Ok(self.query.name.clone())
    }

    fn static_term(&self) -> PyLogicalPlan {
        PyLogicalPlan::from((*self.query.static_term).clone())
    }

    fn recursive_term(&self) -> PyLogicalPlan {
        PyLogicalPlan::from((*self.query.recursive_term).clone())
    }

    fn is_distinct(&self) -> PyResult<bool> {
        Ok(self.query.is_distinct)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("RecursiveQuery({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("RecursiveQuery".to_string())
    }
}

impl LogicalNode for PyRecursiveQuery {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![
            PyLogicalPlan::from((*self.query.static_term).clone()),
            PyLogicalPlan::from((*self.query.recursive_term).clone()),
        ]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
