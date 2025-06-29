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

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;
use async_trait::async_trait;
use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;

use crate::errors::{PyDataFusionError, PyDataFusionResult};
use crate::utils::wait_for_future;
use datafusion::{
    arrow::pyarrow::ToPyArrow,
    catalog::{CatalogProvider, SchemaProvider},
    datasource::{TableProvider, TableType},
};
use datafusion::common::DataFusionError;
use pyo3::Py;

#[pyclass(name = "Catalog", module = "datafusion", subclass)]
pub struct PyCatalog {
    pub catalog: Arc<dyn CatalogProvider>,
}

#[pyclass(name = "Database", module = "datafusion", subclass)]
pub struct PyDatabase {
    pub database: Arc<dyn SchemaProvider>,
}

#[pyclass(name = "Table", module = "datafusion", subclass)]
pub struct PyTable {
    pub table: Arc<dyn TableProvider>,
}

#[derive(Debug)]
#[pyclass(name = "CatalogProvider", module = "datafusion", subclass)]
pub struct PyCatalogProvider {
    py_obj: Py<PyAny>,
}

#[derive(Debug)]
#[pyclass(name = "SchemaProvider", module = "datafusion", subclass)]
pub struct PySchemaProvider {
    py_obj: Py<PyAny>,
}

impl PyCatalogProvider {
    pub fn new(py_obj: Py<PyAny>) -> Self {
        Self { py_obj }
    }
}

impl PySchemaProvider {
    pub fn new(py_obj: Py<PyAny>) -> Self {
        Self { py_obj }
    }
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

    fn table(&self, name: &str, py: Python) -> PyDataFusionResult<PyTable> {
        if let Some(table) = wait_for_future(py, self.database.table(name))?? {
            Ok(PyTable::new(table))
        } else {
            Err(PyDataFusionError::Common(format!(
                "Table not found: {name}"
            )))
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

#[async_trait]
impl SchemaProvider for PySchemaProvider {
    fn owner_name(&self) -> Option<&str> {
        // TODO Find a better way to share the string coming from python because of PyO3
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        Python::with_gil(|py| {
            let obj = self.py_obj.bind_borrowed(py);
            obj.call_method0("table_names")
                .and_then(|res| res.extract::<Vec<String>>())
                .unwrap_or_else(|err| {
                    eprintln!("Error calling table_names: {}", err);
                    vec![]
                })
        })
    }

    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider + 'static>>, DataFusionError>
    {
        Err(DataFusionError::NotImplemented(
            "Python SchemaProvider does not support `table` yet".to_string(),
        ))
    }

    fn table_exist(&self, table_name: &str) -> bool {
        Python::with_gil(|py| {
            let obj = self.py_obj.bind_borrowed(py);
            obj.call_method1("table_exist", (table_name,))
                .and_then(|res| res.extract::<bool>())
                .unwrap_or_else(|err| {
                    eprintln!("Error calling table_exists: {}", err);
                    false
                })
        })
    }

    fn register_table(&self, name: String, table: Arc<dyn TableProvider>) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>>
    {
        Err(DataFusionError::NotImplemented(
            "Python CatalogProvider does not support `register_schema`".to_string(),
        ))
    }

    fn deregister_table(&self, name: &str) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::NotImplemented(
            "Python CatalogProvider does not support `register_schema`".to_string(),
        ))
    }
}

impl CatalogProvider for PyCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        Python::with_gil(|py| {
            let obj = self.py_obj.bind_borrowed(py);
            obj.call_method0("schema_names")
                .and_then(|res| res.extract::<Vec<String>>())
                .unwrap_or_else(|err| {
                    eprintln!("Error calling schema_names: {}", err);
                    vec![]
                })
        })
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Python::with_gil(|py| {
            // let obj = self.py_obj.as_ref(py);
            let obj = self.py_obj.bind_borrowed(py);
            match obj.call_method1("schema", (name,)) {
                Ok(py_schema) => {
                    let schema_provider: PyResult<Py<PyAny>> = py_schema.extract();
                    match schema_provider {
                        Ok(py_obj) => {
                            let rust_provider = Arc::new(PySchemaProvider { py_obj }) as Arc<dyn SchemaProvider>;
                            Some(rust_provider)
                        }
                        Err(err) => {
                            eprintln!("Failed to extract schema provider: {err}");
                            None
                        }
                    }
                }
                Err(err) => {
                    eprintln!("Error calling schema('{}'): {}", name, err);
                    None
                }
            }
        })
    }

    fn register_schema(&self, name: &str, schema: Arc<dyn SchemaProvider>)
        -> datafusion::common::Result<Option<Arc<dyn SchemaProvider>>> {
        Err(DataFusionError::NotImplemented(
            "Python CatalogProvider does not support `register_schema`".to_string(),
        ))
    }

    fn deregister_schema(&self, _name: &str, _cascade: bool)
        -> datafusion::common::Result<Option<Arc<dyn SchemaProvider>>> {
        Err(DataFusionError::NotImplemented(
            "Python CatalogProvider does not support `deregister_schema`".to_string(),
        ))
    }
}
