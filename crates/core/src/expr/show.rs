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
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::pyarrow::PyArrowType;
use datafusion::logical_expr::{Show, ShowKind};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;

use super::logical_node::LogicalNode;
use crate::common::df_schema::PyDFSchema;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(
    from_py_object,
    frozen,
    name = "Show",
    module = "datafusion.expr",
    subclass
)]
#[derive(Clone)]
pub struct PyShow {
    show: Show,
}

impl Display for PyShow {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Show")
    }
}

#[pymethods]
impl PyShow {
    #[new]
    fn new(schema: PyArrowType<Schema>, output_schema: PyDFSchema) -> Self {
        Self {
            show: Show {
                kind: ShowKind::Describe {
                    schema: Arc::new(schema.0),
                },
                output_schema: Arc::new(output_schema.into()),
            },
        }
    }

    pub fn kind(&self) -> &'static str {
        match &self.show.kind {
            ShowKind::Describe { .. } => "describe",
            ShowKind::Tables => "tables",
            ShowKind::Columns { .. } => "columns",
            ShowKind::CreateTable { .. } => "create_table",
            ShowKind::Variables { .. } => "variables",
            ShowKind::Functions { .. } => "functions",
        }
    }

    pub fn schema(&self) -> Option<PyArrowType<Schema>> {
        match &self.show.kind {
            ShowKind::Describe { schema } => Some((**schema).clone().into()),
            _ => None,
        }
    }

    pub fn output_schema(&self) -> PyDFSchema {
        (*self.show.output_schema).clone().into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Show({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("Show".to_string())
    }
}

impl From<PyShow> for Show {
    fn from(show: PyShow) -> Self {
        show.show
    }
}

impl From<Show> for PyShow {
    fn from(show: Show) -> PyShow {
        PyShow { show }
    }
}

impl LogicalNode for PyShow {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
