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

use datafusion::logical_expr::CreateMemoryTable;
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::sql::logical::PyLogicalPlan;

use super::logical_node::LogicalNode;

#[pyclass(name = "CreateMemoryTable", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyCreateMemoryTable {
    create: CreateMemoryTable,
}

impl From<PyCreateMemoryTable> for CreateMemoryTable {
    fn from(create: PyCreateMemoryTable) -> Self {
        create.create
    }
}

impl From<CreateMemoryTable> for PyCreateMemoryTable {
    fn from(create: CreateMemoryTable) -> PyCreateMemoryTable {
        PyCreateMemoryTable { create }
    }
}

impl Display for PyCreateMemoryTable {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CreateMemoryTable
            Name: {:?}
            Input: {:?}
            if_not_exists: {:?}
            or_replace: {:?}",
            &self.create.name,
            &self.create.input,
            &self.create.if_not_exists,
            &self.create.or_replace,
        )
    }
}

#[pymethods]
impl PyCreateMemoryTable {
    fn name(&self) -> PyResult<String> {
        Ok(self.create.name.to_string())
    }

    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    fn if_not_exists(&self) -> bool {
        self.create.if_not_exists
    }

    fn or_replace(&self) -> bool {
        self.create.or_replace
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("CreateMemoryTable({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("CreateMemoryTable".to_string())
    }
}

impl LogicalNode for PyCreateMemoryTable {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.create.input).clone())]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
