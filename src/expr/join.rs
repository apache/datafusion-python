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

use datafusion::logical_expr::logical_plan::{Join, JoinConstraint, JoinType};
use pyo3::{prelude::*, IntoPyObjectExt};
use std::fmt::{self, Display, Formatter};

use crate::common::df_schema::PyDFSchema;
use crate::expr::{logical_node::LogicalNode, PyExpr};
use crate::sql::logical::PyLogicalPlan;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[pyclass(name = "JoinType", module = "datafusion.expr")]
pub struct PyJoinType {
    join_type: JoinType,
}

impl From<JoinType> for PyJoinType {
    fn from(join_type: JoinType) -> PyJoinType {
        PyJoinType { join_type }
    }
}

impl From<PyJoinType> for JoinType {
    fn from(join_type: PyJoinType) -> Self {
        join_type.join_type
    }
}

#[pymethods]
impl PyJoinType {
    pub fn is_outer(&self) -> bool {
        self.join_type.is_outer()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.join_type))
    }
}

impl Display for PyJoinType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.join_type)
    }
}

#[derive(Debug, Clone, Copy)]
#[pyclass(name = "JoinConstraint", module = "datafusion.expr")]
pub struct PyJoinConstraint {
    join_constraint: JoinConstraint,
}

impl From<JoinConstraint> for PyJoinConstraint {
    fn from(join_constraint: JoinConstraint) -> PyJoinConstraint {
        PyJoinConstraint { join_constraint }
    }
}

impl From<PyJoinConstraint> for JoinConstraint {
    fn from(join_constraint: PyJoinConstraint) -> Self {
        join_constraint.join_constraint
    }
}

#[pymethods]
impl PyJoinConstraint {
    fn __repr__(&self) -> PyResult<String> {
        match self.join_constraint {
            JoinConstraint::On => Ok("On".to_string()),
            JoinConstraint::Using => Ok("Using".to_string()),
        }
    }
}

#[pyclass(name = "Join", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyJoin {
    join: Join,
}

impl From<Join> for PyJoin {
    fn from(join: Join) -> PyJoin {
        PyJoin { join }
    }
}

impl From<PyJoin> for Join {
    fn from(join: PyJoin) -> Self {
        join.join
    }
}

impl Display for PyJoin {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Join
            Left: {:?}
            Right: {:?}
            On: {:?}
            Filter: {:?}
            JoinType: {:?}
            JoinConstraint: {:?}
            Schema: {:?}
            NullEqualsNull: {:?}",
            &self.join.left,
            &self.join.right,
            &self.join.on,
            &self.join.filter,
            &self.join.join_type,
            &self.join.join_constraint,
            &self.join.schema,
            &self.join.null_equals_null,
        )
    }
}

#[pymethods]
impl PyJoin {
    /// Retrieves the left input `LogicalPlan` to this `Join` node
    fn left(&self) -> PyResult<PyLogicalPlan> {
        Ok(self.join.left.as_ref().clone().into())
    }

    /// Retrieves the right input `LogicalPlan` to this `Join` node
    fn right(&self) -> PyResult<PyLogicalPlan> {
        Ok(self.join.right.as_ref().clone().into())
    }

    /// Retrieves the right input `LogicalPlan` to this `Join` node
    fn on(&self) -> PyResult<Vec<(PyExpr, PyExpr)>> {
        Ok(self
            .join
            .on
            .iter()
            .map(|(l, r)| (PyExpr::from(l.clone()), PyExpr::from(r.clone())))
            .collect())
    }

    /// Retrieves the filter `Option<PyExpr>` of this `Join` node
    fn filter(&self) -> PyResult<Option<PyExpr>> {
        Ok(self.join.filter.clone().map(Into::into))
    }

    /// Retrieves the `JoinType` to this `Join` node
    fn join_type(&self) -> PyResult<PyJoinType> {
        Ok(self.join.join_type.into())
    }

    /// Retrieves the `JoinConstraint` to this `Join` node
    fn join_constraint(&self) -> PyResult<PyJoinConstraint> {
        Ok(self.join.join_constraint.into())
    }

    /// Resulting Schema for this `Join` node instance
    fn schema(&self) -> PyResult<PyDFSchema> {
        Ok(self.join.schema.as_ref().clone().into())
    }

    /// If null_equals_null is true, null == null else null != null
    fn null_equals_null(&self) -> PyResult<bool> {
        Ok(self.join.null_equals_null)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Join({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("Join".to_string())
    }
}

impl LogicalNode for PyJoin {
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
