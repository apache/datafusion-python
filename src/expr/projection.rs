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

use datafusion::logical_expr::logical_plan::Projection;
use datafusion::logical_expr::Expr;
use pyo3::{prelude::*, IntoPyObjectExt};
use std::fmt::{self, Display, Formatter};

use crate::common::df_schema::PyDFSchema;
use crate::expr::logical_node::LogicalNode;
use crate::expr::PyExpr;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "Projection", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyProjection {
    pub projection: Projection,
}

impl PyProjection {
    pub fn new(projection: Projection) -> Self {
        Self { projection }
    }
}

impl From<Projection> for PyProjection {
    fn from(projection: Projection) -> PyProjection {
        PyProjection { projection }
    }
}

impl From<PyProjection> for Projection {
    fn from(proj: PyProjection) -> Self {
        proj.projection
    }
}

impl Display for PyProjection {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Projection
            \nExpr(s): {:?}
            \nInput: {:?}
            \nProjected Schema: {:?}",
            &self.projection.expr, &self.projection.input, &self.projection.schema,
        )
    }
}

#[pymethods]
impl PyProjection {
    /// Retrieves the expressions for this `Projection`
    fn projections(&self) -> PyResult<Vec<PyExpr>> {
        Ok(self
            .projection
            .expr
            .iter()
            .map(|e| PyExpr::from(e.clone()))
            .collect())
    }

    /// Retrieves the input `LogicalPlan` to this `Projection` node
    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    /// Resulting Schema for this `Projection` node instance
    fn schema(&self) -> PyResult<PyDFSchema> {
        Ok((*self.projection.schema).clone().into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Projection({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("Projection".to_string())
    }
}

impl PyProjection {
    /// Projection: Gets the names of the fields that should be projected
    pub fn projected_expressions(local_expr: &PyExpr) -> Vec<PyExpr> {
        let mut projs: Vec<PyExpr> = Vec::new();
        match &local_expr.expr {
            Expr::Alias(alias) => {
                let py_expr: PyExpr = PyExpr::from(*alias.expr.clone());
                projs.extend_from_slice(Self::projected_expressions(&py_expr).as_slice());
            }
            _ => projs.push(local_expr.clone()),
        }
        projs
    }
}

impl LogicalNode for PyProjection {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.projection.input).clone())]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
