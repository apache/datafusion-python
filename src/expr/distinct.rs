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

use datafusion_expr::Distinct;
use pyo3::prelude::*;

use crate::sql::logical::PyLogicalPlan;

use super::logical_node::LogicalNode;

#[pyclass(name = "Distinct", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyDistinct {
    distinct: Distinct,
}

impl From<PyDistinct> for Distinct {
    fn from(distinct: PyDistinct) -> Self {
        distinct.distinct
    }
}

impl From<Distinct> for PyDistinct {
    fn from(distinct: Distinct) -> PyDistinct {
        PyDistinct { distinct }
    }
}

impl Display for PyDistinct {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Distinct
            \nInput: {:?}",
            self.distinct.input,
        )
    }
}

#[pymethods]
impl PyDistinct {
    /// Retrieves the input `LogicalPlan` to this `Projection` node
    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Distinct({})", self))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("Distinct".to_string())
    }
}

impl LogicalNode for PyDistinct {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.distinct.input).clone())]
    }

    fn to_variant(&self, py: Python) -> PyResult<PyObject> {
        Ok(self.clone().into_py(py))
    }
}
