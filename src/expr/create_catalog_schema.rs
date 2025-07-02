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

use datafusion::logical_expr::CreateCatalogSchema;
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::{common::df_schema::PyDFSchema, sql::logical::PyLogicalPlan};

use super::logical_node::LogicalNode;

#[pyclass(name = "CreateCatalogSchema", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyCreateCatalogSchema {
    create: CreateCatalogSchema,
}

impl From<PyCreateCatalogSchema> for CreateCatalogSchema {
    fn from(create: PyCreateCatalogSchema) -> Self {
        create.create
    }
}

impl From<CreateCatalogSchema> for PyCreateCatalogSchema {
    fn from(create: CreateCatalogSchema) -> PyCreateCatalogSchema {
        PyCreateCatalogSchema { create }
    }
}

impl Display for PyCreateCatalogSchema {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "CreateCatalogSchema: {:?}", self.create.schema_name)
    }
}

#[pymethods]
impl PyCreateCatalogSchema {
    #[new]
    pub fn new(
        schema_name: String,
        if_not_exists: bool,
        schema: PyDFSchema,
    ) -> PyResult<PyCreateCatalogSchema> {
        Ok(PyCreateCatalogSchema {
            create: CreateCatalogSchema {
                schema_name,
                if_not_exists,
                schema: Arc::new(schema.into()),
            },
        })
    }

    pub fn schema_name(&self) -> String {
        self.create.schema_name.clone()
    }

    pub fn if_not_exists(&self) -> bool {
        self.create.if_not_exists
    }

    pub fn schema(&self) -> PyDFSchema {
        (*self.create.schema).clone().into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("CreateCatalogSchema({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("CreateCatalogSchema".to_string())
    }
}

impl LogicalNode for PyCreateCatalogSchema {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
