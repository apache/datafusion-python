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

use datafusion_expr::logical_plan::CrossJoin;
use pyo3::prelude::*;
use std::fmt::{self, Display, Formatter};

use super::logical_node::LogicalNode;
use crate::common::df_schema::PyDFSchema;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "CrossJoin", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyCrossJoin {
    cross_join: CrossJoin,
}

impl From<CrossJoin> for PyCrossJoin {
    fn from(cross_join: CrossJoin) -> PyCrossJoin {
        PyCrossJoin { cross_join }
    }
}

impl From<PyCrossJoin> for CrossJoin {
    fn from(cross_join: PyCrossJoin) -> Self {
        cross_join.cross_join
    }
}

impl Display for PyCrossJoin {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CrossJoin
            \nLeft: {:?}
            \nRight: {:?}
            \nSchema: {:?}",
            &self.cross_join.left, &self.cross_join.right, &self.cross_join.schema
        )
    }
}

#[pymethods]
impl PyCrossJoin {
    /// Retrieves the left input `LogicalPlan` to this `CrossJoin` node
    fn left(&self) -> PyResult<PyLogicalPlan> {
        Ok(self.cross_join.left.as_ref().clone().into())
    }

    /// Retrieves the right input `LogicalPlan` to this `CrossJoin` node
    fn right(&self) -> PyResult<PyLogicalPlan> {
        Ok(self.cross_join.right.as_ref().clone().into())
    }

    /// Resulting Schema for this `CrossJoin` node instance
    fn schema(&self) -> PyResult<PyDFSchema> {
        Ok(self.cross_join.schema.as_ref().clone().into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("CrossJoin({})", self))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("CrossJoin".to_string())
    }
}

impl LogicalNode for PyCrossJoin {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![
            PyLogicalPlan::from((*self.cross_join.left).clone()),
            PyLogicalPlan::from((*self.cross_join.right).clone()),
        ]
    }

    fn to_variant(&self, py: Python) -> PyResult<PyObject> {
        Ok(self.clone().into_py(py))
    }
}
