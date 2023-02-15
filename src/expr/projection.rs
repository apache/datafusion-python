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

use datafusion_expr::logical_plan::Projection;
use pyo3::prelude::*;
use std::fmt::{self, Display, Formatter};

use crate::expr::logical_node::LogicalNode;
use crate::expr::PyExpr;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "Projection", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyProjection {
    projection: Projection,
}

impl From<PyProjection> for Projection {
    fn from(py_proj: PyProjection) -> Projection {
        Projection::try_new_with_schema(
            py_proj.projection.expr,
            py_proj.projection.input.clone(),
            py_proj.projection.schema,
        )
        .unwrap()
    }
}

impl From<Projection> for PyProjection {
    fn from(projection: Projection) -> PyProjection {
        PyProjection { projection }
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
        Ok(LogicalNode::input(self))
    }

    // TODO: Need to uncomment once bindings for `DFSchemaRef` are done.
    // // Resulting Schema for this `Projection` node instance
    // #[pyo3(name = "schema")]
    // fn py_schema(&self) -> PyResult<DFSchemaRef> {
    //     Ok(self.projection.schema)
    // }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Projection({})", self))
    }
}

impl LogicalNode for PyProjection {
    fn input(&self) -> PyLogicalPlan {
        todo!()
    }
}
