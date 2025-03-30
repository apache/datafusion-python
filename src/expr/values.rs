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

use datafusion::logical_expr::Values;
use pyo3::{prelude::*, IntoPyObjectExt};
use pyo3::{pyclass, PyErr, PyResult, Python};

use crate::{common::df_schema::PyDFSchema, sql::logical::PyLogicalPlan};

use super::{logical_node::LogicalNode, PyExpr};

#[pyclass(name = "Values", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyValues {
    values: Values,
}

impl From<Values> for PyValues {
    fn from(values: Values) -> PyValues {
        PyValues { values }
    }
}

impl TryFrom<PyValues> for Values {
    type Error = PyErr;

    fn try_from(py: PyValues) -> Result<Self, Self::Error> {
        Ok(py.values)
    }
}

impl LogicalNode for PyValues {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}

#[pymethods]
impl PyValues {
    #[new]
    pub fn new(schema: PyDFSchema, values: Vec<Vec<PyExpr>>) -> PyResult<Self> {
        let values = values
            .into_iter()
            .map(|row| row.into_iter().map(|expr| expr.into()).collect())
            .collect();
        Ok(PyValues {
            values: Values {
                schema: Arc::new(schema.into()),
                values,
            },
        })
    }

    pub fn schema(&self) -> PyResult<PyDFSchema> {
        Ok((*self.values.schema).clone().into())
    }

    pub fn values(&self) -> Vec<Vec<PyExpr>> {
        self.values
            .values
            .clone()
            .into_iter()
            .map(|row| row.into_iter().map(|expr| expr.into()).collect())
            .collect()
    }
}
