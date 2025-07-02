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

use datafusion::common::DataFusionError;
use datafusion::logical_expr::logical_plan::Sort;
use pyo3::{prelude::*, IntoPyObjectExt};
use std::fmt::{self, Display, Formatter};

use crate::common::df_schema::PyDFSchema;
use crate::expr::logical_node::LogicalNode;
use crate::expr::sort_expr::PySortExpr;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "Sort", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PySort {
    sort: Sort,
}

impl From<Sort> for PySort {
    fn from(sort: Sort) -> PySort {
        PySort { sort }
    }
}

impl TryFrom<PySort> for Sort {
    type Error = DataFusionError;

    fn try_from(agg: PySort) -> Result<Self, Self::Error> {
        Ok(agg.sort)
    }
}

impl Display for PySort {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Sort
            \nExpr(s): {:?}
            \nInput: {:?}
            \nSchema: {:?}",
            &self.sort.expr,
            self.sort.input,
            self.sort.input.schema()
        )
    }
}

#[pymethods]
impl PySort {
    /// Retrieves the sort expressions for this `Sort`
    fn sort_exprs(&self) -> PyResult<Vec<PySortExpr>> {
        Ok(self
            .sort
            .expr
            .iter()
            .map(|e| PySortExpr::from(e.clone()))
            .collect())
    }

    fn get_fetch_val(&self) -> PyResult<Option<usize>> {
        Ok(self.sort.fetch)
    }

    /// Retrieves the input `LogicalPlan` to this `Sort` node
    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    /// Resulting Schema for this `Sort` node instance
    fn schema(&self) -> PyDFSchema {
        self.sort.input.schema().as_ref().clone().into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Sort({self})"))
    }
}

impl LogicalNode for PySort {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.sort.input).clone())]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
