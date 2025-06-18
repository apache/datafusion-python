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

use pyo3::{pyclass, pymethods, Bound, PyResult, Python};
use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::{
    catalog::{
        CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider, TableProvider,
    },
    common::exec_err,
    datasource::MemTable,
    error::{DataFusionError, Result},
};
use datafusion_ffi::catalog_provider::FFI_CatalogProvider;
use pyo3::types::PyCapsule;

pub fn my_table() -> Arc<dyn TableProvider + 'static> {
    use arrow::datatypes::{DataType, Field};
    use datafusion::common::record_batch;

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

#[derive(Debug)]
pub struct FixedSchemaProvider {
    inner: MemorySchemaProvider,
}

impl Default for FixedSchemaProvider {
    fn default() -> Self {
        let inner = MemorySchemaProvider::new();

        let table = my_table();

        let _ = inner.register_table("my_table".to_string(), table).unwrap();

        Self { inner }
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
#[derive(Debug)]
pub(crate) struct MyCatalogProvider {
    inner: MemoryCatalogProvider,
}

impl Default for MyCatalogProvider {
    fn default() -> Self {
        let inner = MemoryCatalogProvider::new();

        let schema_name: &str = "my_schema";
        let _ = inner.register_schema(schema_name, Arc::new(FixedSchemaProvider::default()));

        Self { inner }
    }
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
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }

    pub fn __datafusion_catalog_provider__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_catalog_provider".into();
        let catalog_provider =
            FFI_CatalogProvider::new(Arc::new(MyCatalogProvider::default()), None);

        PyCapsule::new(py, catalog_provider, Some(name))
    }
}
