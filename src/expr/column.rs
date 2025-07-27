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

use datafusion::common::Column;
use pyo3::prelude::*;

#[pyclass(name = "Column", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyColumn {
    pub col: Column,
}

impl PyColumn {
    pub fn new(col: Column) -> Self {
        Self { col }
    }
}

impl From<Column> for PyColumn {
    fn from(col: Column) -> PyColumn {
        PyColumn { col }
    }
}

#[pymethods]
impl PyColumn {
    /// Get the column name
    fn name(&self) -> String {
        self.col.name.clone()
    }

    /// Get the column relation
    fn relation(&self) -> Option<String> {
        self.col.relation.as_ref().map(|r| format!("{r}"))
    }

    /// Get the fully-qualified column name
    fn qualified_name(&self) -> String {
        self.col.flat_name()
    }

    /// Get a String representation of this column
    fn __repr__(&self) -> String {
        self.qualified_name()
    }
}
