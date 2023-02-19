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

use datafusion_expr::logical_plan::Filter;
use pyo3::prelude::*;
use std::fmt::{self, Display, Formatter};

use crate::common::df_schema::PyDFSchema;
use crate::expr::logical_node::LogicalNode;
use crate::expr::PyExpr;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "Filter", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyFilter {
    filter: Filter,
}

impl From<Filter> for PyFilter {
    fn from(filter: Filter) -> PyFilter {
        PyFilter { filter }
    }
}

impl From<PyFilter> for Filter {
    fn from(filter: PyFilter) -> Self {
        filter.filter
    }
}

impl Display for PyFilter {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Filter
            \nPredicate: {:?}
            \Input: {:?}",
            &self.filter.predicate, &self.filter.input
        )
    }
}

#[pymethods]
impl PyFilter {
    /// Retrieves the predicate expression for this `Filter`
    fn predicate(&self) -> PyResult<PyExpr> {
        Ok(PyExpr::from(self.filter.predicate.clone()))
    }

    /// Retrieves the input `LogicalPlan` to this `Filter` node
    fn input(&self) -> PyLogicalPlan {
        PyLogicalPlan::from((*self.filter.input).clone())
    }

    /// Resulting Schema for this `Filter` node instance
    fn schema(&self) -> PyResult<PyDFSchema> {
        Ok(self.filter.input.schema().as_ref().clone().into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Filter({})", self))
    }
}

impl LogicalNode for PyFilter {
    fn input(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.filter.input).clone())]
    }
}
