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

use datafusion_common::DataFusionError;
use datafusion_expr::logical_plan::Projection;
use pyo3::prelude::*;
use std::fmt::{self, Display, Formatter};

use crate::common::df_schema::PyDFSchema;
use crate::errors::py_runtime_err;
use crate::expr::logical_node::LogicalNode;
use crate::expr::PyExpr;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "Projection", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyProjection {
    projection: Projection,
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

impl TryFrom<PyProjection> for Projection {
    type Error = DataFusionError;

    fn try_from(py_proj: PyProjection) -> Result<Self, Self::Error> {
        Projection::try_new_with_schema(
            py_proj.projection.expr,
            py_proj.projection.input.clone(),
            py_proj.projection.schema,
        )
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
    #[pyo3(name = "projections")]
    fn py_projections(&self) -> PyResult<Vec<PyExpr>> {
        Ok(self
            .projection
            .expr
            .iter()
            .map(|e| PyExpr::from(e.clone()))
            .collect())
    }

    // Retrieves the input `LogicalPlan` to this `Projection` node
    #[pyo3(name = "input")]
    fn py_input(&self) -> PyResult<PyLogicalPlan> {
        // DataFusion make a loose guarantee that each Projection should have an input, however
        // we check for that hear since we are performing explicit index retrieval
        let inputs = LogicalNode::input(self);
        if !inputs.is_empty() {
            return Ok(inputs[0].clone());
        }

        Err(py_runtime_err(format!(
            "Expected `input` field for Projection node: {}",
            self
        )))
    }

    // Resulting Schema for this `Projection` node instance
    #[pyo3(name = "schema")]
    fn py_schema(&self) -> PyResult<PyDFSchema> {
        Ok((*self.projection.schema).clone().into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Projection({})", self))
    }
}

impl LogicalNode for PyProjection {
    fn name(&self) -> &str {
        "Projection"
    }

    fn input(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.projection.input).clone())]
    }
}
