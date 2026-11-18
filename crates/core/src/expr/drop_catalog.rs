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

use datafusion::logical_expr::DropCatalog;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;

use super::logical_node::LogicalNode;
use crate::common::df_schema::PyDFSchema;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(
    from_py_object,
    frozen,
    name = "DropCatalog",
    module = "datafusion.expr",
    subclass
)]
#[derive(Clone)]
pub struct PyDropCatalog {
    drop: DropCatalog,
}

impl From<PyDropCatalog> for DropCatalog {
    fn from(drop: PyDropCatalog) -> Self {
        drop.drop
    }
}

impl From<DropCatalog> for PyDropCatalog {
    fn from(drop: DropCatalog) -> PyDropCatalog {
        PyDropCatalog { drop }
    }
}

impl Display for PyDropCatalog {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DropCatalog: name={}", self.drop.name)
    }
}

#[pymethods]
impl PyDropCatalog {
    fn name(&self) -> String {
        self.drop.name.clone()
    }

    fn if_exists(&self) -> bool {
        self.drop.if_exists
    }

    fn cascade(&self) -> bool {
        self.drop.cascade
    }

    fn schema(&self) -> PyDFSchema {
        (*self.drop.schema).clone().into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("DropCatalog({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("DropCatalog".to_string())
    }
}

impl LogicalNode for PyDropCatalog {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
