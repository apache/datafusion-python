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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider, TableProviderFactory};
use datafusion_common::error::Result as DataFusionResult;
use datafusion_expr::CreateExternalTable;
use datafusion_ffi::table_provider_factory::FFI_TableProviderFactory;
use pyo3::types::PyCapsule;
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};

use crate::catalog_provider;
use crate::utils::ffi_logical_codec_from_pycapsule;

#[derive(Debug)]
pub(crate) struct ExampleTableProviderFactory {}

impl ExampleTableProviderFactory {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl TableProviderFactory for ExampleTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        _cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        Ok(catalog_provider::my_table())
    }
}

#[pyclass(
    name = "MyTableProviderFactory",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Debug)]
pub struct MyTableProviderFactory {
    inner: Arc<ExampleTableProviderFactory>,
}

impl Default for MyTableProviderFactory {
    fn default() -> Self {
        let inner = Arc::new(ExampleTableProviderFactory::new());
        Self { inner }
    }
}

#[pymethods]
impl MyTableProviderFactory {
    #[new]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn __datafusion_table_provider_factory__<'py>(
        &self,
        py: Python<'py>,
        codec: Bound<PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_table_provider_factory".into();
        let codec = ffi_logical_codec_from_pycapsule(codec)?;
        let factory = Arc::clone(&self.inner) as Arc<dyn TableProviderFactory + Send>;
        let factory = FFI_TableProviderFactory::new_with_ffi_codec(factory, None, codec);

        PyCapsule::new(py, factory, Some(name))
    }
}
