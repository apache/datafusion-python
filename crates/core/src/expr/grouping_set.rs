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

use datafusion::logical_expr::{Expr, GroupingSet};
use pyo3::prelude::*;

use crate::expr::PyExpr;

#[pyclass(
    from_py_object,
    frozen,
    name = "GroupingSet",
    module = "datafusion.expr",
    subclass
)]
#[derive(Clone)]
pub struct PyGroupingSet {
    grouping_set: GroupingSet,
}

#[pymethods]
impl PyGroupingSet {
    #[staticmethod]
    #[pyo3(signature = (*exprs))]
    fn rollup(exprs: Vec<PyExpr>) -> PyExpr {
        Expr::GroupingSet(GroupingSet::Rollup(
            exprs.into_iter().map(|e| e.expr).collect(),
        ))
        .into()
    }

    #[staticmethod]
    #[pyo3(signature = (*exprs))]
    fn cube(exprs: Vec<PyExpr>) -> PyExpr {
        Expr::GroupingSet(GroupingSet::Cube(
            exprs.into_iter().map(|e| e.expr).collect(),
        ))
        .into()
    }

    #[staticmethod]
    #[pyo3(signature = (*expr_lists))]
    fn grouping_sets(expr_lists: Vec<Vec<PyExpr>>) -> PyExpr {
        Expr::GroupingSet(GroupingSet::GroupingSets(
            expr_lists
                .into_iter()
                .map(|list| list.into_iter().map(|e| e.expr).collect())
                .collect(),
        ))
        .into()
    }
}

impl From<PyGroupingSet> for GroupingSet {
    fn from(grouping_set: PyGroupingSet) -> Self {
        grouping_set.grouping_set
    }
}

impl From<GroupingSet> for PyGroupingSet {
    fn from(grouping_set: GroupingSet) -> PyGroupingSet {
        PyGroupingSet { grouping_set }
    }
}
