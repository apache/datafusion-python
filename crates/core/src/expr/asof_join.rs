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

use datafusion::logical_expr::logical_plan::{AsOfJoin, AsOfMatch};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;

use crate::common::df_schema::PyDFSchema;
use crate::expr::PyExpr;
use crate::expr::join::PyJoinConstraint;
use crate::expr::logical_node::LogicalNode;
use crate::sql::logical::PyLogicalPlan;

#[derive(Debug, Clone)]
#[pyclass(from_py_object, frozen, name = "AsOfMatch", module = "datafusion.expr")]
pub struct PyAsOfMatch {
    match_condition: AsOfMatch,
}

impl From<AsOfMatch> for PyAsOfMatch {
    fn from(match_condition: AsOfMatch) -> PyAsOfMatch {
        PyAsOfMatch { match_condition }
    }
}

impl From<PyAsOfMatch> for AsOfMatch {
    fn from(match_condition: PyAsOfMatch) -> Self {
        match_condition.match_condition
    }
}

#[pymethods]
impl PyAsOfMatch {
    fn left(&self) -> PyResult<PyExpr> {
        Ok(self.match_condition.left.clone().into())
    }

    fn op(&self) -> PyResult<String> {
        Ok(self.match_condition.op.to_string())
    }

    fn right(&self) -> PyResult<PyExpr> {
        Ok(self.match_condition.right.clone().into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.match_condition))
    }
}

impl Display for PyAsOfMatch {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.match_condition)
    }
}

#[pyclass(
    from_py_object,
    frozen,
    name = "AsOfJoin",
    module = "datafusion.expr",
    subclass
)]
#[derive(Clone)]
pub struct PyAsOfJoin {
    join: AsOfJoin,
}

impl From<AsOfJoin> for PyAsOfJoin {
    fn from(join: AsOfJoin) -> PyAsOfJoin {
        PyAsOfJoin { join }
    }
}

impl From<PyAsOfJoin> for AsOfJoin {
    fn from(join: PyAsOfJoin) -> Self {
        join.join
    }
}

impl Display for PyAsOfJoin {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "AsOfJoin
            Left: {:?}
            Right: {:?}
            On: {:?}
            MatchCondition: {:?}
            JoinConstraint: {:?}
            Schema: {:?}",
            self.join.left,
            self.join.right,
            self.join.on,
            self.join.match_condition,
            self.join.join_constraint,
            self.join.schema,
        )
    }
}

#[pymethods]
impl PyAsOfJoin {
    /// Retrieves the left input `LogicalPlan` to this `AsOfJoin` node
    fn left(&self) -> PyResult<PyLogicalPlan> {
        Ok(self.join.left.as_ref().clone().into())
    }

    /// Retrieves the right input `LogicalPlan` to this `AsOfJoin` node
    fn right(&self) -> PyResult<PyLogicalPlan> {
        Ok(self.join.right.as_ref().clone().into())
    }

    /// Retrieves the equality keys of this `AsOfJoin` node
    fn on(&self) -> PyResult<Vec<(PyExpr, PyExpr)>> {
        Ok(self
            .join
            .on
            .iter()
            .map(|(l, r)| (PyExpr::from(l.clone()), PyExpr::from(r.clone())))
            .collect())
    }

    /// Retrieves the `AsOfMatch` condition of this `AsOfJoin` node
    fn match_condition(&self) -> PyResult<PyAsOfMatch> {
        Ok(self.join.match_condition.as_ref().clone().into())
    }

    /// Retrieves the `JoinConstraint` to this `AsOfJoin` node
    fn join_constraint(&self) -> PyResult<PyJoinConstraint> {
        Ok(self.join.join_constraint.into())
    }

    /// Resulting Schema for this `AsOfJoin` node instance
    fn schema(&self) -> PyResult<PyDFSchema> {
        Ok(self.join.schema.as_ref().clone().into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("AsOfJoin({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("AsOfJoin".to_string())
    }
}

impl LogicalNode for PyAsOfJoin {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![
            PyLogicalPlan::from((*self.join.left).clone()),
            PyLogicalPlan::from((*self.join.right).clone()),
        ]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
