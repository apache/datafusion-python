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
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableProvider;
use datafusion_ffi::schema_provider::{FFI_SchemaProvider, ForeignSchemaProvider};
use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use pyo3::IntoPyObjectExt;

use crate::dataset::Dataset;
use crate::errors::{py_datafusion_err, to_datafusion_err, PyDataFusionError, PyDataFusionResult};
use crate::table::PyTable;
use crate::utils::{validate_pycapsule, wait_for_future};

#[pyclass(frozen, name = "RawCatalog", module = "datafusion.catalog", subclass)]
#[derive(Clone)]
pub struct PyCatalog {
    pub catalog: Arc<dyn CatalogProvider>,
}

#[pyclass(frozen, name = "RawSchema", module = "datafusion.catalog", subclass)]
#[derive(Clone)]
pub struct PySchema {
    pub schema: Arc<dyn SchemaProvider>,
}

impl From<Arc<dyn CatalogProvider>> for PyCatalog {
    fn from(catalog: Arc<dyn CatalogProvider>) -> Self {
        Self { catalog }
    }
}

impl From<Arc<dyn SchemaProvider>> for PySchema {
    fn from(schema: Arc<dyn SchemaProvider>) -> Self {
        Self { schema }
    }
}

#[pymethods]
impl PyCatalog {
    #[new]
    fn new(catalog: Py<PyAny>) -> Self {
        let catalog_provider =
            Arc::new(RustWrappedPyCatalogProvider::new(catalog)) as Arc<dyn CatalogProvider>;
        catalog_provider.into()
    }

    #[staticmethod]
    fn memory_catalog() -> Self {
        let catalog_provider =
            Arc::new(MemoryCatalogProvider::default()) as Arc<dyn CatalogProvider>;
        catalog_provider.into()
    }

    fn schema_names(&self) -> HashSet<String> {
        self.catalog.schema_names().into_iter().collect()
    }

    #[pyo3(signature = (name="public"))]
    fn schema(&self, name: &str) -> PyResult<Py<PyAny>> {
        let schema = self
            .catalog
            .schema(name)
            .ok_or(PyKeyError::new_err(format!(
                "Schema with name {name} doesn't exist."
            )))?;

        Python::attach(|py| {
            match schema
                .as_any()
                .downcast_ref::<RustWrappedPySchemaProvider>()
            {
                Some(wrapped_schema) => Ok(wrapped_schema.schema_provider.clone_ref(py)),
                None => PySchema::from(schema).into_py_any(py),
            }
        })
    }

    fn register_schema(&self, name: &str, schema_provider: Bound<'_, PyAny>) -> PyResult<()> {
        let provider = if schema_provider.hasattr("__datafusion_schema_provider__")? {
            let capsule = schema_provider
                .getattr("__datafusion_schema_provider__")?
                .call0()?;
            let capsule = capsule.downcast::<PyCapsule>().map_err(py_datafusion_err)?;
            validate_pycapsule(capsule, "datafusion_schema_provider")?;

            let provider = unsafe { capsule.reference::<FFI_SchemaProvider>() };
            let provider: ForeignSchemaProvider = provider.into();
            Arc::new(provider) as Arc<dyn SchemaProvider>
        } else {
            match schema_provider.extract::<PySchema>() {
                Ok(py_schema) => py_schema.schema,
                Err(_) => Arc::new(RustWrappedPySchemaProvider::new(schema_provider.into()))
                    as Arc<dyn SchemaProvider>,
            }
        };

        let _ = self
            .catalog
            .register_schema(name, provider)
            .map_err(py_datafusion_err)?;

        Ok(())
    }

    fn deregister_schema(&self, name: &str, cascade: bool) -> PyResult<()> {
        let _ = self
            .catalog
            .deregister_schema(name, cascade)
            .map_err(py_datafusion_err)?;

        Ok(())
    }

    fn __repr__(&self) -> PyResult<String> {
        let mut names: Vec<String> = self.schema_names().into_iter().collect();
        names.sort();
        Ok(format!("Catalog(schema_names=[{}])", names.join(", ")))
    }
}

#[pymethods]
impl PySchema {
    #[new]
    fn new(schema_provider: Py<PyAny>) -> Self {
        let schema_provider =
            Arc::new(RustWrappedPySchemaProvider::new(schema_provider)) as Arc<dyn SchemaProvider>;
        schema_provider.into()
    }

    #[staticmethod]
    fn memory_schema() -> Self {
        let schema_provider = Arc::new(MemorySchemaProvider::default()) as Arc<dyn SchemaProvider>;
        schema_provider.into()
    }

    #[getter]
    fn table_names(&self) -> HashSet<String> {
        self.schema.table_names().into_iter().collect()
    }

    fn table(&self, name: &str, py: Python) -> PyDataFusionResult<PyTable> {
        if let Some(table) = wait_for_future(py, self.schema.table(name))?? {
            Ok(PyTable::from(table))
        } else {
            Err(PyDataFusionError::Common(format!(
                "Table not found: {name}"
            )))
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        let mut names: Vec<String> = self.table_names().into_iter().collect();
        names.sort();
        Ok(format!("Schema(table_names=[{}])", names.join(";")))
    }

    fn register_table(&self, name: &str, table_provider: &Bound<'_, PyAny>) -> PyResult<()> {
        let table = PyTable::new(table_provider)?;

        let _ = self
            .schema
            .register_table(name.to_string(), table.table)
            .map_err(py_datafusion_err)?;

        Ok(())
    }

    fn deregister_table(&self, name: &str) -> PyResult<()> {
        let _ = self
            .schema
            .deregister_table(name)
            .map_err(py_datafusion_err)?;

        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct RustWrappedPySchemaProvider {
    schema_provider: Py<PyAny>,
    owner_name: Option<String>,
}

impl RustWrappedPySchemaProvider {
    pub fn new(schema_provider: Py<PyAny>) -> Self {
        let owner_name = Python::attach(|py| {
            schema_provider
                .bind(py)
                .getattr("owner_name")
                .ok()
                .map(|name| name.to_string())
        });

        Self {
            schema_provider,
            owner_name,
        }
    }

    fn table_inner(&self, name: &str) -> PyResult<Option<Arc<dyn TableProvider>>> {
        Python::attach(|py| {
            let provider = self.schema_provider.bind(py);
            let py_table_method = provider.getattr("table")?;

            let py_table = py_table_method.call((name,), None)?;
            if py_table.is_none() {
                return Ok(None);
            }

            let table = PyTable::new(&py_table)?;

            Ok(Some(table.table))
        })
    }
}

#[async_trait]
impl SchemaProvider for RustWrappedPySchemaProvider {
    fn owner_name(&self) -> Option<&str> {
        self.owner_name.as_deref()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        Python::attach(|py| {
            let provider = self.schema_provider.bind(py);

            provider
                .getattr("table_names")
                .and_then(|names| names.extract::<Vec<String>>())
                .unwrap_or_else(|err| {
                    log::error!("Unable to get table_names: {err}");
                    Vec::default()
                })
        })
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        self.table_inner(name).map_err(to_datafusion_err)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let py_table = PyTable::from(table);
        Python::attach(|py| {
            let provider = self.schema_provider.bind(py);
            let _ = provider
                .call_method1("register_table", (name, py_table))
                .map_err(to_datafusion_err)?;
            // Since the definition of `register_table` says that an error
            // will be returned if the table already exists, there is no
            // case where we want to return a table provider as output.
            Ok(None)
        })
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        Python::attach(|py| {
            let provider = self.schema_provider.bind(py);
            let table = provider
                .call_method1("deregister_table", (name,))
                .map_err(to_datafusion_err)?;
            if table.is_none() {
                return Ok(None);
            }

            // If we can turn this table provider into a `Dataset`, return it.
            // Otherwise, return None.
            let dataset = match Dataset::new(&table, py) {
                Ok(dataset) => Some(Arc::new(dataset) as Arc<dyn TableProvider>),
                Err(_) => None,
            };

            Ok(dataset)
        })
    }

    fn table_exist(&self, name: &str) -> bool {
        Python::attach(|py| {
            let provider = self.schema_provider.bind(py);
            provider
                .call_method1("table_exist", (name,))
                .and_then(|pyobj| pyobj.extract())
                .unwrap_or(false)
        })
    }
}

#[derive(Debug)]
pub(crate) struct RustWrappedPyCatalogProvider {
    pub(crate) catalog_provider: Py<PyAny>,
}

impl RustWrappedPyCatalogProvider {
    pub fn new(catalog_provider: Py<PyAny>) -> Self {
        Self { catalog_provider }
    }

    fn schema_inner(&self, name: &str) -> PyResult<Option<Arc<dyn SchemaProvider>>> {
        Python::attach(|py| {
            let provider = self.catalog_provider.bind(py);

            let py_schema = provider.call_method1("schema", (name,))?;
            if py_schema.is_none() {
                return Ok(None);
            }

            if py_schema.hasattr("__datafusion_schema_provider__")? {
                let capsule = provider
                    .getattr("__datafusion_schema_provider__")?
                    .call0()?;
                let capsule = capsule.downcast::<PyCapsule>().map_err(py_datafusion_err)?;
                validate_pycapsule(capsule, "datafusion_schema_provider")?;

                let provider = unsafe { capsule.reference::<FFI_SchemaProvider>() };
                let provider: ForeignSchemaProvider = provider.into();

                Ok(Some(Arc::new(provider) as Arc<dyn SchemaProvider>))
            } else {
                if let Ok(inner_schema) = py_schema.getattr("schema") {
                    if let Ok(inner_schema) = inner_schema.extract::<PySchema>() {
                        return Ok(Some(inner_schema.schema));
                    }
                }
                match py_schema.extract::<PySchema>() {
                    Ok(inner_schema) => Ok(Some(inner_schema.schema)),
                    Err(_) => {
                        let py_schema = RustWrappedPySchemaProvider::new(py_schema.into());

                        Ok(Some(Arc::new(py_schema) as Arc<dyn SchemaProvider>))
                    }
                }
            }
        })
    }
}

#[async_trait]
impl CatalogProvider for RustWrappedPyCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        Python::attach(|py| {
            let provider = self.catalog_provider.bind(py);
            provider
                .getattr("schema_names")
                .and_then(|names| names.extract::<Vec<String>>())
                .unwrap_or_else(|err| {
                    log::error!("Unable to get schema_names: {err}");
                    Vec::default()
                })
        })
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schema_inner(name).unwrap_or_else(|err| {
            log::error!("CatalogProvider schema returned error: {err}");
            None
        })
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn SchemaProvider>>> {
        // JRIGHT HERE
        // let py_schema: PySchema = schema.into();
        Python::attach(|py| {
            let py_schema = match schema
                .as_any()
                .downcast_ref::<RustWrappedPySchemaProvider>()
            {
                Some(wrapped_schema) => wrapped_schema.schema_provider.as_any(),
                None => &PySchema::from(schema)
                    .into_py_any(py)
                    .map_err(to_datafusion_err)?,
            };

            let provider = self.catalog_provider.bind(py);
            let schema = provider
                .call_method1("register_schema", (name, py_schema))
                .map_err(to_datafusion_err)?;
            if schema.is_none() {
                return Ok(None);
            }

            let schema = Arc::new(RustWrappedPySchemaProvider::new(schema.into()))
                as Arc<dyn SchemaProvider>;

            Ok(Some(schema))
        })
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> datafusion::common::Result<Option<Arc<dyn SchemaProvider>>> {
        Python::attach(|py| {
            let provider = self.catalog_provider.bind(py);
            let schema = provider
                .call_method1("deregister_schema", (name, cascade))
                .map_err(to_datafusion_err)?;
            if schema.is_none() {
                return Ok(None);
            }

            let schema = Arc::new(RustWrappedPySchemaProvider::new(schema.into()))
                as Arc<dyn SchemaProvider>;

            Ok(Some(schema))
        })
    }
}

pub(crate) fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCatalog>()?;
    m.add_class::<PySchema>()?;
    m.add_class::<PyTable>()?;

    Ok(())
}
