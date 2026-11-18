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

use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};

use datafusion::logical_expr::CreateExternalCatalog;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;

use super::logical_node::LogicalNode;
use crate::common::df_schema::PyDFSchema;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(
    from_py_object,
    frozen,
    name = "CreateExternalCatalog",
    module = "datafusion.expr",
    subclass
)]
#[derive(Clone)]
pub struct PyCreateExternalCatalog {
    create: CreateExternalCatalog,
}

impl From<PyCreateExternalCatalog> for CreateExternalCatalog {
    fn from(create: PyCreateExternalCatalog) -> Self {
        create.create
    }
}

impl From<CreateExternalCatalog> for PyCreateExternalCatalog {
    fn from(create: CreateExternalCatalog) -> PyCreateExternalCatalog {
        PyCreateExternalCatalog { create }
    }
}

impl Display for PyCreateExternalCatalog {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CreateExternalCatalog: name={} catalog_type={}",
            self.create.catalog_name, self.create.catalog_type
        )
    }
}

#[pymethods]
impl PyCreateExternalCatalog {
    fn catalog_name(&self) -> String {
        self.create.catalog_name.clone()
    }

    fn catalog_type(&self) -> String {
        self.create.catalog_type.clone()
    }

    fn location(&self) -> Option<String> {
        self.create.location.clone()
    }

    fn if_not_exists(&self) -> bool {
        self.create.if_not_exists
    }

    fn or_replace(&self) -> bool {
        self.create.or_replace
    }

    fn options(&self) -> HashMap<String, String> {
        self.create.options.clone()
    }

    fn schema(&self) -> PyDFSchema {
        (*self.create.schema).clone().into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("CreateExternalCatalog({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("CreateExternalCatalog".to_string())
    }
}

impl LogicalNode for PyCreateExternalCatalog {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
