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

use crate::{common::df_schema::PyDFSchema, sql::logical::PyLogicalPlan};
use datafusion::logical_expr::EmptyRelation;
use pyo3::{prelude::*, IntoPyObjectExt};
use std::fmt::{self, Display, Formatter};

use super::logical_node::LogicalNode;

#[pyclass(name = "EmptyRelation", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyEmptyRelation {
    empty: EmptyRelation,
}

impl From<PyEmptyRelation> for EmptyRelation {
    fn from(empty_relation: PyEmptyRelation) -> Self {
        empty_relation.empty
    }
}

impl From<EmptyRelation> for PyEmptyRelation {
    fn from(empty: EmptyRelation) -> PyEmptyRelation {
        PyEmptyRelation { empty }
    }
}

impl Display for PyEmptyRelation {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Empty Relation
            Produce One Row: {:?}
            Schema: {:?}",
            &self.empty.produce_one_row, &self.empty.schema
        )
    }
}

#[pymethods]
impl PyEmptyRelation {
    fn produce_one_row(&self) -> PyResult<bool> {
        Ok(self.empty.produce_one_row)
    }

    /// Resulting Schema for this `EmptyRelation` node instance
    fn schema(&self) -> PyResult<PyDFSchema> {
        Ok((*self.empty.schema).clone().into())
    }

    /// Get a String representation of this column
    fn __repr__(&self) -> String {
        format!("{self}")
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("EmptyRelation".to_string())
    }
}

impl LogicalNode for PyEmptyRelation {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        // table scans are leaf nodes and do not have inputs
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
