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

use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use arrow::{datatypes::Schema, pyarrow::PyArrowType};
use datafusion::logical_expr::DescribeTable;
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::{common::df_schema::PyDFSchema, sql::logical::PyLogicalPlan};

use super::logical_node::LogicalNode;

#[pyclass(name = "DescribeTable", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyDescribeTable {
    describe: DescribeTable,
}

impl Display for PyDescribeTable {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DescribeTable")
    }
}

#[pymethods]
impl PyDescribeTable {
    #[new]
    fn new(schema: PyArrowType<Schema>, output_schema: PyDFSchema) -> Self {
        Self {
            describe: DescribeTable {
                schema: Arc::new(schema.0),
                output_schema: Arc::new(output_schema.into()),
            },
        }
    }

    pub fn schema(&self) -> PyArrowType<Schema> {
        (*self.describe.schema).clone().into()
    }

    pub fn output_schema(&self) -> PyDFSchema {
        (*self.describe.output_schema).clone().into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("DescribeTable({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("DescribeTable".to_string())
    }
}

impl From<PyDescribeTable> for DescribeTable {
    fn from(describe: PyDescribeTable) -> Self {
        describe.describe
    }
}

impl From<DescribeTable> for PyDescribeTable {
    fn from(describe: DescribeTable) -> PyDescribeTable {
        PyDescribeTable { describe }
    }
}

impl LogicalNode for PyDescribeTable {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
