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

use datafusion::logical_expr::CreateCatalog;
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::{common::df_schema::PyDFSchema, sql::logical::PyLogicalPlan};

use super::logical_node::LogicalNode;

#[pyclass(name = "CreateCatalog", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyCreateCatalog {
    create: CreateCatalog,
}

impl From<PyCreateCatalog> for CreateCatalog {
    fn from(create: PyCreateCatalog) -> Self {
        create.create
    }
}

impl From<CreateCatalog> for PyCreateCatalog {
    fn from(create: CreateCatalog) -> PyCreateCatalog {
        PyCreateCatalog { create }
    }
}

impl Display for PyCreateCatalog {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "CreateCatalog: {:?}", self.create.catalog_name)
    }
}

#[pymethods]
impl PyCreateCatalog {
    #[new]
    pub fn new(
        catalog_name: String,
        if_not_exists: bool,
        schema: PyDFSchema,
    ) -> PyResult<PyCreateCatalog> {
        Ok(PyCreateCatalog {
            create: CreateCatalog {
                catalog_name,
                if_not_exists,
                schema: Arc::new(schema.into()),
            },
        })
    }

    pub fn catalog_name(&self) -> String {
        self.create.catalog_name.clone()
    }

    pub fn if_not_exists(&self) -> bool {
        self.create.if_not_exists
    }

    pub fn schema(&self) -> PyDFSchema {
        (*self.create.schema).clone().into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("CreateCatalog({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("CreateCatalog".to_string())
    }
}

impl LogicalNode for PyCreateCatalog {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
