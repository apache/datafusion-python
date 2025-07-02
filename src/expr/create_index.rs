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

use datafusion::logical_expr::CreateIndex;
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::{common::df_schema::PyDFSchema, sql::logical::PyLogicalPlan};

use super::{logical_node::LogicalNode, sort_expr::PySortExpr};

#[pyclass(name = "CreateIndex", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyCreateIndex {
    create: CreateIndex,
}

impl From<PyCreateIndex> for CreateIndex {
    fn from(create: PyCreateIndex) -> Self {
        create.create
    }
}

impl From<CreateIndex> for PyCreateIndex {
    fn from(create: CreateIndex) -> PyCreateIndex {
        PyCreateIndex { create }
    }
}

impl Display for PyCreateIndex {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "CreateIndex: {:?}", self.create.name)
    }
}

#[pymethods]
impl PyCreateIndex {
    #[new]
    #[pyo3(signature = (table, columns, unique, if_not_exists, schema, name=None, using=None))]
    pub fn new(
        table: String,
        columns: Vec<PySortExpr>,
        unique: bool,
        if_not_exists: bool,
        schema: PyDFSchema,
        name: Option<String>,
        using: Option<String>,
    ) -> PyResult<Self> {
        Ok(PyCreateIndex {
            create: CreateIndex {
                name,
                table: table.into(),
                using,
                columns: columns.iter().map(|c| c.clone().into()).collect(),
                unique,
                if_not_exists,
                schema: Arc::new(schema.into()),
            },
        })
    }

    pub fn name(&self) -> Option<String> {
        self.create.name.clone()
    }

    pub fn table(&self) -> PyResult<String> {
        Ok(self.create.table.to_string())
    }

    pub fn using(&self) -> Option<String> {
        self.create.using.clone()
    }

    pub fn columns(&self) -> Vec<PySortExpr> {
        self.create
            .columns
            .iter()
            .map(|c| c.clone().into())
            .collect()
    }

    pub fn unique(&self) -> bool {
        self.create.unique
    }

    pub fn if_not_exists(&self) -> bool {
        self.create.if_not_exists
    }

    pub fn schema(&self) -> PyDFSchema {
        (*self.create.schema).clone().into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("CreateIndex({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("CreateIndex".to_string())
    }
}

impl LogicalNode for PyCreateIndex {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
