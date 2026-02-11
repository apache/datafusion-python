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
use std::fmt::Debug;
use std::sync::Arc;

use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion_catalog::{
    CatalogProvider, CatalogProviderList, MemTable, MemoryCatalogProvider,
    MemoryCatalogProviderList, MemorySchemaProvider, SchemaProvider, TableProvider,
};
use datafusion_common::error::{DataFusionError, Result};
use datafusion_ffi::catalog_provider::FFI_CatalogProvider;
use datafusion_ffi::catalog_provider_list::FFI_CatalogProviderList;
use datafusion_ffi::schema_provider::FFI_SchemaProvider;
use pyo3::types::PyCapsule;
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};

use crate::utils::ffi_logical_codec_from_pycapsule;

pub fn my_table() -> Arc<dyn TableProvider + 'static> {
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::record_batch;

    let schema = Arc::new(Schema::new(vec![
        Field::new("units", DataType::Int32, true),
        Field::new("price", DataType::Float64, true),
    ]));

    let partitions = vec![
        record_batch!(
            ("units", Int32, vec![10, 20, 30]),
            ("price", Float64, vec![1.0, 2.0, 5.0])
        )
        .unwrap(),
        record_batch!(
            ("units", Int32, vec![5, 7]),
            ("price", Float64, vec![1.5, 2.5])
        )
        .unwrap(),
    ];

    Arc::new(MemTable::try_new(schema, vec![partitions]).unwrap())
}

#[pyclass(
    name = "FixedSchemaProvider",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Debug)]
pub struct FixedSchemaProvider {
    inner: Arc<MemorySchemaProvider>,
}

impl Default for FixedSchemaProvider {
    fn default() -> Self {
        let inner = Arc::new(MemorySchemaProvider::new());

        let table = my_table();

        let _ = inner.register_table("my_table".to_string(), table).unwrap();

        Self { inner }
    }
}

#[pymethods]
impl FixedSchemaProvider {
    #[new]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn __datafusion_schema_provider__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_schema_provider".into();

        let provider = Arc::clone(&self.inner) as Arc<dyn SchemaProvider + Send>;

        let codec = ffi_logical_codec_from_pycapsule(session)?;
        let provider = FFI_SchemaProvider::new_with_ffi_codec(provider, None, codec);

        PyCapsule::new(py, provider, Some(name))
    }
}

#[async_trait]
impl SchemaProvider for FixedSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        self.inner.table(name).await
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}

/// This catalog provider is intended only for unit tests. It prepopulates with one
/// schema and only allows for schemas named after four types of fruit.
#[pyclass(
    name = "MyCatalogProvider",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Debug, Clone)]
pub(crate) struct MyCatalogProvider {
    inner: Arc<MemoryCatalogProvider>,
}

impl CatalogProvider for MyCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.inner.schema(name)
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        self.inner.register_schema(name, schema)
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        self.inner.deregister_schema(name, cascade)
    }
}

#[pymethods]
impl MyCatalogProvider {
    #[new]
    pub fn new() -> PyResult<Self> {
        let inner = Arc::new(MemoryCatalogProvider::new());

        let schema_name: &str = "my_schema";
        let _ = inner.register_schema(schema_name, Arc::new(FixedSchemaProvider::default()));

        Ok(Self { inner })
    }

    pub fn __datafusion_catalog_provider__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_catalog_provider".into();

        let provider = Arc::clone(&self.inner) as Arc<dyn CatalogProvider + Send>;

        let codec = ffi_logical_codec_from_pycapsule(session)?;
        let provider = FFI_CatalogProvider::new_with_ffi_codec(provider, None, codec);

        PyCapsule::new(py, provider, Some(name))
    }
}

/// This catalog provider list is intended only for unit tests.
/// It pre-populates with a single catalog.
#[pyclass(
    name = "MyCatalogProviderList",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Debug, Clone)]
pub(crate) struct MyCatalogProviderList {
    inner: Arc<MemoryCatalogProviderList>,
}

impl CatalogProviderList for MyCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn catalog_names(&self) -> Vec<String> {
        self.inner.catalog_names()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.inner.catalog(name)
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.inner.register_catalog(name, catalog)
    }
}

#[pymethods]
impl MyCatalogProviderList {
    #[new]
    pub fn new() -> PyResult<Self> {
        let inner = Arc::new(MemoryCatalogProviderList::new());

        inner.register_catalog(
            "auto_ffi_catalog".to_owned(),
            Arc::new(MyCatalogProvider::new()?),
        );

        Ok(Self { inner })
    }

    pub fn __datafusion_catalog_provider_list__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_catalog_provider_list".into();

        let provider = Arc::clone(&self.inner) as Arc<dyn CatalogProviderList + Send>;

        let codec = ffi_logical_codec_from_pycapsule(session)?;
        let provider = FFI_CatalogProviderList::new_with_ffi_codec(provider, None, codec);

        PyCapsule::new(py, provider, Some(name))
    }
}
