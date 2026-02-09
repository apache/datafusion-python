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
    CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList,
    MemorySchemaProvider, SchemaProvider,
};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableProvider;
use datafusion_ffi::catalog_provider::FFI_CatalogProvider;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::schema_provider::FFI_SchemaProvider;
use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use pyo3::IntoPyObjectExt;

use crate::dataset::Dataset;
use crate::errors::{py_datafusion_err, to_datafusion_err, PyDataFusionError, PyDataFusionResult};
use crate::table::PyTable;
use crate::utils::{
    create_logical_extension_capsule, extract_logical_extension_codec, validate_pycapsule,
    wait_for_future,
};

#[pyclass(
    frozen,
    name = "RawCatalogList",
    module = "datafusion.catalog",
    subclass
)]
#[derive(Clone)]
pub struct PyCatalogList {
    pub catalog_list: Arc<dyn CatalogProviderList>,
    codec: Arc<FFI_LogicalExtensionCodec>,
}

#[pyclass(frozen, name = "RawCatalog", module = "datafusion.catalog", subclass)]
#[derive(Clone)]
pub struct PyCatalog {
    pub catalog: Arc<dyn CatalogProvider>,
    codec: Arc<FFI_LogicalExtensionCodec>,
}

#[pyclass(frozen, name = "RawSchema", module = "datafusion.catalog", subclass)]
#[derive(Clone)]
pub struct PySchema {
    pub schema: Arc<dyn SchemaProvider>,
    codec: Arc<FFI_LogicalExtensionCodec>,
}

impl PyCatalog {
    pub(crate) fn new_from_parts(
        catalog: Arc<dyn CatalogProvider>,
        codec: Arc<FFI_LogicalExtensionCodec>,
    ) -> Self {
        Self { catalog, codec }
    }
}

impl PySchema {
    pub(crate) fn new_from_parts(
        schema: Arc<dyn SchemaProvider>,
        codec: Arc<FFI_LogicalExtensionCodec>,
    ) -> Self {
        Self { schema, codec }
    }
}

#[pymethods]
impl PyCatalogList {
    #[new]
    pub fn new(
        py: Python,
        catalog_list: Py<PyAny>,
        session: Option<Bound<PyAny>>,
    ) -> PyResult<Self> {
        let codec = extract_logical_extension_codec(py, session)?;
        let catalog_list = Arc::new(RustWrappedPyCatalogProviderList::new(
            catalog_list,
            codec.clone(),
        )) as Arc<dyn CatalogProviderList>;
        Ok(Self {
            catalog_list,
            codec,
        })
    }

    #[staticmethod]
    pub fn memory_catalog_list(py: Python, session: Option<Bound<PyAny>>) -> PyResult<Self> {
        let codec = extract_logical_extension_codec(py, session)?;
        let catalog_list =
            Arc::new(MemoryCatalogProviderList::default()) as Arc<dyn CatalogProviderList>;
        Ok(Self {
            catalog_list,
            codec,
        })
    }

    pub fn catalog_names(&self) -> HashSet<String> {
        self.catalog_list.catalog_names().into_iter().collect()
    }

    #[pyo3(signature = (name="public"))]
    pub fn catalog(&self, name: &str) -> PyResult<Py<PyAny>> {
        let catalog = self
            .catalog_list
            .catalog(name)
            .ok_or(PyKeyError::new_err(format!(
                "Schema with name {name} doesn't exist."
            )))?;

        Python::attach(|py| {
            match catalog
                .as_any()
                .downcast_ref::<RustWrappedPyCatalogProvider>()
            {
                Some(wrapped_catalog) => Ok(wrapped_catalog.catalog_provider.clone_ref(py)),
                None => PyCatalog::new_from_parts(catalog, self.codec.clone()).into_py_any(py),
            }
        })
    }

    pub fn register_catalog(&self, name: &str, catalog_provider: Bound<'_, PyAny>) -> PyResult<()> {
        let provider = extract_catalog_provider_from_pyobj(catalog_provider, self.codec.as_ref())?;

        let _ = self
            .catalog_list
            .register_catalog(name.to_owned(), provider);

        Ok(())
    }

    pub fn __repr__(&self) -> PyResult<String> {
        let mut names: Vec<String> = self.catalog_names().into_iter().collect();
        names.sort();
        Ok(format!("CatalogList(catalog_names=[{}])", names.join(", ")))
    }
}

#[pymethods]
impl PyCatalog {
    #[new]
    pub fn new(py: Python, catalog: Py<PyAny>, session: Option<Bound<PyAny>>) -> PyResult<Self> {
        let codec = extract_logical_extension_codec(py, session)?;
        let catalog = Arc::new(RustWrappedPyCatalogProvider::new(catalog, codec.clone()))
            as Arc<dyn CatalogProvider>;
        Ok(Self { catalog, codec })
    }

    #[staticmethod]
    pub fn memory_catalog(py: Python, session: Option<Bound<PyAny>>) -> PyResult<Self> {
        let codec = extract_logical_extension_codec(py, session)?;
        let catalog = Arc::new(MemoryCatalogProvider::default()) as Arc<dyn CatalogProvider>;
        Ok(Self { catalog, codec })
    }

    pub fn schema_names(&self) -> HashSet<String> {
        self.catalog.schema_names().into_iter().collect()
    }

    #[pyo3(signature = (name="public"))]
    pub fn schema(&self, name: &str) -> PyResult<Py<PyAny>> {
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
                None => PySchema::new_from_parts(schema, self.codec.clone()).into_py_any(py),
            }
        })
    }

    pub fn register_schema(&self, name: &str, schema_provider: Bound<'_, PyAny>) -> PyResult<()> {
        let provider = extract_schema_provider_from_pyobj(schema_provider, self.codec.as_ref())?;

        let _ = self
            .catalog
            .register_schema(name, provider)
            .map_err(py_datafusion_err)?;

        Ok(())
    }

    pub fn deregister_schema(&self, name: &str, cascade: bool) -> PyResult<()> {
        let _ = self
            .catalog
            .deregister_schema(name, cascade)
            .map_err(py_datafusion_err)?;

        Ok(())
    }

    pub fn __repr__(&self) -> PyResult<String> {
        let mut names: Vec<String> = self.schema_names().into_iter().collect();
        names.sort();
        Ok(format!("Catalog(schema_names=[{}])", names.join(", ")))
    }
}

#[pymethods]
impl PySchema {
    #[new]
    pub fn new(
        py: Python,
        schema_provider: Py<PyAny>,
        session: Option<Bound<PyAny>>,
    ) -> PyResult<Self> {
        let codec = extract_logical_extension_codec(py, session)?;
        let schema =
            Arc::new(RustWrappedPySchemaProvider::new(schema_provider)) as Arc<dyn SchemaProvider>;
        Ok(Self { schema, codec })
    }

    #[staticmethod]
    fn memory_schema(py: Python, session: Option<Bound<PyAny>>) -> PyResult<Self> {
        let codec = extract_logical_extension_codec(py, session)?;
        let schema = Arc::new(MemorySchemaProvider::default()) as Arc<dyn SchemaProvider>;
        Ok(Self { schema, codec })
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

    fn register_table(&self, name: &str, table_provider: Bound<'_, PyAny>) -> PyResult<()> {
        let py = table_provider.py();
        let codec_capsule = create_logical_extension_capsule(py, self.codec.as_ref())?
            .as_any()
            .clone();

        let table = PyTable::new(table_provider, Some(codec_capsule))?;

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

    fn table_exist(&self, name: &str) -> bool {
        self.schema.table_exist(name)
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

            let table = PyTable::new(py_table, None)?;

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
    codec: Arc<FFI_LogicalExtensionCodec>,
}

impl RustWrappedPyCatalogProvider {
    pub fn new(catalog_provider: Py<PyAny>, codec: Arc<FFI_LogicalExtensionCodec>) -> Self {
        Self {
            catalog_provider,
            codec,
        }
    }

    fn schema_inner(&self, name: &str) -> PyResult<Option<Arc<dyn SchemaProvider>>> {
        Python::attach(|py| {
            let provider = self.catalog_provider.bind(py);

            let py_schema = provider.call_method1("schema", (name,))?;
            if py_schema.is_none() {
                return Ok(None);
            }

            extract_schema_provider_from_pyobj(py_schema, self.codec.as_ref()).map(Some)
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
                .call_method0("schema_names")
                .and_then(|names| names.extract::<HashSet<String>>())
                .map(|names| names.into_iter().collect())
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
        Python::attach(|py| {
            let py_schema = match schema
                .as_any()
                .downcast_ref::<RustWrappedPySchemaProvider>()
            {
                Some(wrapped_schema) => wrapped_schema.schema_provider.as_any(),
                None => &PySchema::new_from_parts(schema, self.codec.clone())
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

#[derive(Debug)]
pub(crate) struct RustWrappedPyCatalogProviderList {
    pub(crate) catalog_provider_list: Py<PyAny>,
    codec: Arc<FFI_LogicalExtensionCodec>,
}

impl RustWrappedPyCatalogProviderList {
    pub fn new(catalog_provider_list: Py<PyAny>, codec: Arc<FFI_LogicalExtensionCodec>) -> Self {
        Self {
            catalog_provider_list,
            codec,
        }
    }

    fn catalog_inner(&self, name: &str) -> PyResult<Option<Arc<dyn CatalogProvider>>> {
        Python::attach(|py| {
            let provider = self.catalog_provider_list.bind(py);

            let py_schema = provider.call_method1("catalog", (name,))?;
            if py_schema.is_none() {
                return Ok(None);
            }

            extract_catalog_provider_from_pyobj(py_schema, self.codec.as_ref()).map(Some)
        })
    }
}

#[async_trait]
impl CatalogProviderList for RustWrappedPyCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn catalog_names(&self) -> Vec<String> {
        Python::attach(|py| {
            let provider = self.catalog_provider_list.bind(py);
            provider
                .call_method0("catalog_names")
                .and_then(|names| names.extract::<HashSet<String>>())
                .map(|names| names.into_iter().collect())
                .unwrap_or_else(|err| {
                    log::error!("Unable to get catalog_names: {err}");
                    Vec::default()
                })
        })
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalog_inner(name).unwrap_or_else(|err| {
            log::error!("CatalogProvider catalog returned error: {err}");
            None
        })
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        Python::attach(|py| {
            let py_catalog = match catalog
                .as_any()
                .downcast_ref::<RustWrappedPyCatalogProvider>()
            {
                Some(wrapped_schema) => wrapped_schema.catalog_provider.as_any().clone_ref(py),
                None => {
                    match PyCatalog::new_from_parts(catalog, self.codec.clone()).into_py_any(py) {
                        Ok(c) => c,
                        Err(err) => {
                            log::error!(
                                "register_catalog returned error during conversion to PyAny: {err}"
                            );
                            return None;
                        }
                    }
                }
            };

            let provider = self.catalog_provider_list.bind(py);
            let catalog = match provider.call_method1("register_catalog", (name, py_catalog)) {
                Ok(c) => c,
                Err(err) => {
                    log::error!("register_catalog returned error: {err}");
                    return None;
                }
            };
            if catalog.is_none() {
                return None;
            }

            let catalog = Arc::new(RustWrappedPyCatalogProvider::new(
                catalog.into(),
                self.codec.clone(),
            )) as Arc<dyn CatalogProvider>;

            Some(catalog)
        })
    }
}

fn extract_catalog_provider_from_pyobj(
    mut catalog_provider: Bound<PyAny>,
    codec: &FFI_LogicalExtensionCodec,
) -> PyResult<Arc<dyn CatalogProvider>> {
    if catalog_provider.hasattr("__datafusion_catalog_provider__")? {
        let py = catalog_provider.py();
        let codec_capsule = create_logical_extension_capsule(py, codec)?;
        catalog_provider = catalog_provider
            .getattr("__datafusion_catalog_provider__")?
            .call1((codec_capsule,))?;
    }

    let provider = if let Ok(capsule) = catalog_provider.downcast::<PyCapsule>() {
        validate_pycapsule(capsule, "datafusion_catalog_provider")?;

        let provider = unsafe { capsule.reference::<FFI_CatalogProvider>() };
        let provider: Arc<dyn CatalogProvider + Send> = provider.into();
        provider as Arc<dyn CatalogProvider>
    } else {
        match catalog_provider.extract::<PyCatalog>() {
            Ok(py_catalog) => py_catalog.catalog,
            Err(_) => Arc::new(RustWrappedPyCatalogProvider::new(
                catalog_provider.into(),
                Arc::new(codec.clone()),
            )) as Arc<dyn CatalogProvider>,
        }
    };

    Ok(provider)
}

fn extract_schema_provider_from_pyobj(
    mut schema_provider: Bound<PyAny>,
    codec: &FFI_LogicalExtensionCodec,
) -> PyResult<Arc<dyn SchemaProvider>> {
    if schema_provider.hasattr("__datafusion_schema_provider__")? {
        let py = schema_provider.py();
        let codec_capsule = create_logical_extension_capsule(py, codec)?;
        schema_provider = schema_provider
            .getattr("__datafusion_schema_provider__")?
            .call1((codec_capsule,))?;
    }

    let provider = if let Ok(capsule) = schema_provider.downcast::<PyCapsule>() {
        validate_pycapsule(capsule, "datafusion_schema_provider")?;

        let provider = unsafe { capsule.reference::<FFI_SchemaProvider>() };
        let provider: Arc<dyn SchemaProvider + Send> = provider.into();
        provider as Arc<dyn SchemaProvider>
    } else {
        match schema_provider.extract::<PySchema>() {
            Ok(py_schema) => py_schema.schema,
            Err(_) => Arc::new(RustWrappedPySchemaProvider::new(schema_provider.into()))
                as Arc<dyn SchemaProvider>,
        }
    };

    Ok(provider)
}

pub(crate) fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCatalog>()?;
    m.add_class::<PySchema>()?;
    m.add_class::<PyTable>()?;

    Ok(())
}
