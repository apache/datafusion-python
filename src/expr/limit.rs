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

use datafusion::logical_expr::logical_plan::Limit;
use pyo3::{prelude::*, IntoPyObjectExt};
use std::fmt::{self, Display, Formatter};

use crate::common::df_schema::PyDFSchema;
use crate::expr::logical_node::LogicalNode;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "Limit", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyLimit {
    limit: Limit,
}

impl From<Limit> for PyLimit {
    fn from(limit: Limit) -> PyLimit {
        PyLimit { limit }
    }
}

impl From<PyLimit> for Limit {
    fn from(limit: PyLimit) -> Self {
        limit.limit
    }
}

impl Display for PyLimit {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Limit
            Skip: {:?}
            Fetch: {:?}
            Input: {:?}",
            &self.limit.skip, &self.limit.fetch, &self.limit.input
        )
    }
}

#[pymethods]
impl PyLimit {
    // NOTE: Upstream now has expressions for skip and fetch
    // TODO: Do we still want to expose these?
    // REF: https://github.com/apache/datafusion/pull/12836

    // /// Retrieves the skip value for this `Limit`
    // fn skip(&self) -> usize {
    //     self.limit.skip
    // }

    // /// Retrieves the fetch value for this `Limit`
    // fn fetch(&self) -> Option<usize> {
    //     self.limit.fetch
    // }

    /// Retrieves the input `LogicalPlan` to this `Limit` node
    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    /// Resulting Schema for this `Limit` node instance
    fn schema(&self) -> PyResult<PyDFSchema> {
        Ok(self.limit.input.schema().as_ref().clone().into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Limit({self})"))
    }
}

impl LogicalNode for PyLimit {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.limit.input).clone())]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
