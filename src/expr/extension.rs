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

use datafusion::logical_expr::Extension;
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::sql::logical::PyLogicalPlan;

use super::logical_node::LogicalNode;

#[pyclass(name = "Extension", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyExtension {
    pub node: Extension,
}

impl From<Extension> for PyExtension {
    fn from(node: Extension) -> PyExtension {
        PyExtension { node }
    }
}

#[pymethods]
impl PyExtension {
    fn name(&self) -> PyResult<String> {
        Ok(self.node.node.name().to_string())
    }
}

impl LogicalNode for PyExtension {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
