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

use datafusion::logical_expr::logical_plan::DropTable;
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::sql::logical::PyLogicalPlan;

use super::logical_node::LogicalNode;

#[pyclass(name = "DropTable", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyDropTable {
    drop: DropTable,
}

impl From<PyDropTable> for DropTable {
    fn from(drop: PyDropTable) -> Self {
        drop.drop
    }
}

impl From<DropTable> for PyDropTable {
    fn from(drop: DropTable) -> PyDropTable {
        PyDropTable { drop }
    }
}

impl Display for PyDropTable {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "DropTable
            name: {:?}
            if_exists: {:?}
            schema: {:?}",
            &self.drop.name, &self.drop.if_exists, &self.drop.schema,
        )
    }
}

#[pymethods]
impl PyDropTable {
    fn name(&self) -> PyResult<String> {
        Ok(self.drop.name.to_string())
    }

    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    fn if_exists(&self) -> bool {
        self.drop.if_exists
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("DropTable({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("DropTable".to_string())
    }
}

impl LogicalNode for PyDropTable {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
