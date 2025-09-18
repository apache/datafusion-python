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

use std::ffi::CString;
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion_ffi::table_provider::{FFI_TableProvider, ForeignTableProvider};
use pyo3::exceptions::PyDeprecationWarning;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict};

use crate::catalog::PyTable;
use crate::dataframe::PyDataFrame;
use crate::errors::{py_datafusion_err, PyDataFusionResult};
use crate::utils::{get_tokio_runtime, validate_pycapsule};

/// Represents a table provider that can be registered with DataFusion
#[pyclass(name = "TableProvider", module = "datafusion")]
#[derive(Clone)]
pub struct PyTableProvider {
    pub(crate) provider: Arc<dyn TableProvider>,
}

impl PyTableProvider {
    pub(crate) fn new(provider: Arc<dyn TableProvider>) -> Self {
        Self { provider }
    }

    /// Return a `PyTable` wrapper around this provider.
    ///
    /// Historically callers chained `as_table().table()` to access the
    /// underlying [`Arc<dyn TableProvider>`]. Prefer [`as_arc`] or
    /// [`into_inner`] for direct access instead.
    pub fn as_table(&self) -> PyTable {
        PyTable::new(Arc::clone(&self.provider))
    }

    /// Return a clone of the inner [`TableProvider`].
    pub fn as_arc(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.provider)
    }

    /// Consume this wrapper and return the inner [`TableProvider`].
    pub fn into_inner(self) -> Arc<dyn TableProvider> {
        self.provider
    }
}

#[pymethods]
impl PyTableProvider {
    /// Create a `TableProvider` from a PyCapsule containing an FFI pointer
    #[staticmethod]
    pub fn from_capsule(capsule: Bound<'_, PyAny>) -> PyResult<Self> {
        let capsule = capsule.downcast::<PyCapsule>().map_err(py_datafusion_err)?;
        validate_pycapsule(capsule, "datafusion_table_provider")?;

        let provider = unsafe { capsule.reference::<FFI_TableProvider>() };
        let provider: ForeignTableProvider = provider.into();

        Ok(Self::new(Arc::new(provider)))
    }

    /// Create a `TableProvider` from a `DataFrame`.
    ///
    /// This method simply delegates to `DataFrame.into_view`.
    #[staticmethod]
    pub fn from_dataframe(df: &PyDataFrame) -> Self {
        // Clone the inner DataFrame and convert it into a view TableProvider.
        // `into_view` consumes a DataFrame, so clone the underlying DataFrame
        Self::new(df.inner_df().as_ref().clone().into_view())
    }

    /// Create a `TableProvider` from a `DataFrame` by converting it into a view.
    ///
    /// Deprecated: prefer `DataFrame.into_view` or
    /// `TableProvider.from_dataframe` instead.
    #[staticmethod]
    pub fn from_view(py: Python<'_>, df: &PyDataFrame) -> PyDataFusionResult<Self> {
        let kwargs = PyDict::new(py);
        // Keep stack level consistent with python/datafusion/table_provider.py
        kwargs.set_item("stacklevel", 2)?;
        py.import("warnings")?.call_method(
            "warn",
            (
                "PyTableProvider.from_view() is deprecated; use DataFrame.into_view() or TableProvider.from_dataframe() instead.",
                py.get_type::<PyDeprecationWarning>(),
            ),
            Some(&kwargs),
        )?;
        Ok(Self::from_dataframe(df))
    }

    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = CString::new("datafusion_table_provider").unwrap();

        let runtime = get_tokio_runtime().0.handle().clone();
        let provider: Arc<dyn TableProvider + Send> = self.provider.clone();
        let provider = FFI_TableProvider::new(provider, false, Some(runtime));

        PyCapsule::new(py, provider, Some(name.clone()))
    }
}
