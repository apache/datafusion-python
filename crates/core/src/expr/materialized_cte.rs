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

use datafusion::logical_expr::logical_plan::{MaterializedCte, MaterializedCteScan};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;

use crate::common::df_schema::PyDFSchema;
use crate::expr::logical_node::LogicalNode;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(
    from_py_object,
    frozen,
    name = "MaterializedCte",
    module = "datafusion.expr",
    subclass
)]
#[derive(Clone)]
pub struct PyMaterializedCte {
    cte: MaterializedCte,
}

impl From<MaterializedCte> for PyMaterializedCte {
    fn from(cte: MaterializedCte) -> Self {
        Self { cte }
    }
}

impl From<PyMaterializedCte> for MaterializedCte {
    fn from(cte: PyMaterializedCte) -> Self {
        cte.cte
    }
}

impl Display for PyMaterializedCte {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "MaterializedCte: name={} id={}",
            self.cte.name, self.cte.id
        )
    }
}

#[pymethods]
impl PyMaterializedCte {
    fn name(&self) -> String {
        self.cte.name.clone()
    }

    fn id(&self) -> u64 {
        self.cte.id.as_u64()
    }

    fn cte(&self) -> PyLogicalPlan {
        PyLogicalPlan::from((*self.cte.cte).clone())
    }

    fn continuation(&self) -> PyLogicalPlan {
        PyLogicalPlan::from((*self.cte.continuation).clone())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("MaterializedCte({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("MaterializedCte".to_string())
    }
}

impl LogicalNode for PyMaterializedCte {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![
            PyLogicalPlan::from((*self.cte.cte).clone()),
            PyLogicalPlan::from((*self.cte.continuation).clone()),
        ]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}

#[pyclass(
    from_py_object,
    frozen,
    name = "MaterializedCteScan",
    module = "datafusion.expr",
    subclass
)]
#[derive(Clone)]
pub struct PyMaterializedCteScan {
    scan: MaterializedCteScan,
}

impl From<MaterializedCteScan> for PyMaterializedCteScan {
    fn from(scan: MaterializedCteScan) -> Self {
        Self { scan }
    }
}

impl From<PyMaterializedCteScan> for MaterializedCteScan {
    fn from(scan: PyMaterializedCteScan) -> Self {
        scan.scan
    }
}

impl Display for PyMaterializedCteScan {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "MaterializedCteScan: name={} id={}",
            self.scan.name, self.scan.id
        )
    }
}

#[pymethods]
impl PyMaterializedCteScan {
    fn name(&self) -> String {
        self.scan.name.clone()
    }

    fn id(&self) -> u64 {
        self.scan.id.as_u64()
    }

    fn schema(&self) -> PyDFSchema {
        self.scan.schema.as_ref().clone().into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("MaterializedCteScan({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("MaterializedCteScan".to_string())
    }
}

impl LogicalNode for PyMaterializedCteScan {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
