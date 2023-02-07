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

use std::collections::HashSet;
use std::sync::Arc;

use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;

use crate::errors::DataFusionError;
use crate::utils::wait_for_future;
use datafusion::{
    arrow::pyarrow::PyArrowConvert,
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    datasource::{TableProvider, TableType},
};

#[pyclass(name = "Catalog", module = "datafusion", subclass)]
pub(crate) struct PyCatalog {
    catalog: Arc<dyn CatalogProvider>,
}

#[pyclass(name = "Database", module = "datafusion", subclass)]
pub(crate) struct PyDatabase {
    database: Arc<dyn SchemaProvider>,
}

#[pyclass(name = "Table", module = "datafusion", subclass)]
pub struct PyTable {
    table: Arc<dyn TableProvider>,
}

impl PyCatalog {
    pub fn new(catalog: Arc<dyn CatalogProvider>) -> Self {
        Self { catalog }
    }
}

impl PyDatabase {
    pub fn new(database: Arc<dyn SchemaProvider>) -> Self {
        Self { database }
    }
}

impl PyTable {
    pub fn new(table: Arc<dyn TableProvider>) -> Self {
        Self { table }
    }

    pub fn table(&self) -> Arc<dyn TableProvider> {
        self.table.clone()
    }
}

#[pymethods]
impl PyCatalog {
    fn names(&self) -> Vec<String> {
        self.catalog.schema_names()
    }

    #[pyo3(signature = (name="public"))]
    fn database(&self, name: &str) -> PyResult<PyDatabase> {
        match self.catalog.schema(name) {
            Some(database) => Ok(PyDatabase::new(database)),
            None => Err(PyKeyError::new_err(format!(
                "Database with name {name} doesn't exist."
            ))),
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "Catalog(schema_names=[{}])",
            self.names().join(";")
        ))
    }
}

#[pymethods]
impl PyDatabase {
    fn names(&self) -> HashSet<String> {
        self.database.table_names().into_iter().collect()
    }

    fn table(&self, name: &str, py: Python) -> PyResult<PyTable> {
        if let Some(table) = wait_for_future(py, self.database.table(name)) {
            Ok(PyTable::new(table))
        } else {
            Err(DataFusionError::Common(format!("Table not found: {name}")).into())
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "Database(table_names=[{}])",
            Vec::from_iter(self.names()).join(";")
        ))
    }

    // register_table
    // deregister_table
}

#[pymethods]
impl PyTable {
    /// Get a reference to the schema for this table
    #[getter]
    fn schema(&self, py: Python) -> PyResult<PyObject> {
        self.table.schema().to_pyarrow(py)
    }

    /// Get the type of this table for metadata/catalog purposes.
    #[getter]
    fn kind(&self) -> &str {
        match self.table.table_type() {
            TableType::Base => "physical",
            TableType::View => "view",
            TableType::Temporary => "temporary",
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        let kind = self.kind();
        Ok(format!("Table(kind={kind})"))
    }

    // fn scan
    // fn statistics
    // fn has_exact_statistics
    // fn supports_filter_pushdown
}
