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
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::pyarrow::ToPyArrow;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::common::Column;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{
    CreateExternalTable, Expr, LogicalPlanBuilder, TableProviderFilterPushDown,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::DataFrame;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_python_util::{create_logical_extension_capsule, table_provider_from_pycapsule};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;

use crate::context::PySessionContext;
use crate::dataframe::PyDataFrame;
use crate::dataset::Dataset;
use crate::errors;
use crate::expr::create_external_table::PyCreateExternalTable;

/// This struct is used as a common method for all TableProviders,
/// whether they refer to an FFI provider, an internally known
/// implementation, a dataset, or a dataframe view.
#[pyclass(
    from_py_object,
    frozen,
    name = "RawTable",
    module = "datafusion.catalog",
    subclass
)]
#[derive(Clone)]
pub struct PyTable {
    pub table: Arc<dyn TableProvider>,
}

impl PyTable {
    pub fn table(&self) -> Arc<dyn TableProvider> {
        self.table.clone()
    }
}

#[pymethods]
impl PyTable {
    /// Instantiate from any Python object that supports any of the table
    /// types. We do not know a priori when using this method if the object
    /// will be passed a wrapped or raw class. Here we handle all of the
    /// following object types:
    ///
    /// - PyTable (essentially a clone operation), but either raw or wrapped
    /// - DataFrame, either raw or wrapped
    /// - FFI Table Providers via PyCapsule
    /// - PyArrow Dataset objects
    #[new]
    pub fn new(obj: Bound<'_, PyAny>, session: Option<Bound<PyAny>>) -> PyResult<Self> {
        let py = obj.py();
        if let Ok(py_table) = obj.extract::<PyTable>() {
            Ok(py_table)
        } else if let Ok(py_table) = obj
            .getattr("_inner")
            .and_then(|inner| inner.extract::<PyTable>().map_err(Into::<PyErr>::into))
        {
            Ok(py_table)
        } else if let Ok(py_df) = obj.extract::<PyDataFrame>() {
            let provider = py_df.inner_df().as_ref().clone().into_view();
            Ok(PyTable::from(provider))
        } else if let Ok(py_df) = obj
            .getattr("df")
            .and_then(|inner| inner.extract::<PyDataFrame>().map_err(Into::<PyErr>::into))
        {
            let provider = py_df.inner_df().as_ref().clone().into_view();
            Ok(PyTable::from(provider))
        } else if let Some(provider) = {
            let session = match session {
                Some(session) => session,
                None => PySessionContext::global_ctx()?.into_bound_py_any(obj.py())?,
            };
            table_provider_from_pycapsule(obj.clone(), session)?
        } {
            Ok(PyTable::from(provider))
        } else {
            let provider = Arc::new(Dataset::new(&obj, py)?) as Arc<dyn TableProvider>;
            Ok(PyTable::from(provider))
        }
    }

    /// Get a reference to the schema for this table
    #[getter]
    fn schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
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
}

impl From<Arc<dyn TableProvider>> for PyTable {
    fn from(table: Arc<dyn TableProvider>) -> Self {
        Self { table }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TempViewTable {
    df: Arc<DataFrame>,
}

/// This is nearly identical to `DataFrameTableProvider`
/// except that it is for temporary tables.
/// Remove when https://github.com/apache/datafusion/issues/18026
/// closes.
impl TempViewTable {
    pub(crate) fn new(df: Arc<DataFrame>) -> Self {
        Self { df }
    }
}

#[async_trait]
impl TableProvider for TempViewTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(self.df.schema().as_arrow().clone())
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let filter = filters.iter().cloned().reduce(|acc, new| acc.and(new));
        let plan = self.df.logical_plan().clone();
        let mut plan = LogicalPlanBuilder::from(plan);

        if let Some(filter) = filter {
            plan = plan.filter(filter)?;
        }

        let mut plan = if let Some(projection) = projection {
            // avoiding adding a redundant projection (e.g. SELECT * FROM view)
            let current_projection = (0..plan.schema().fields().len()).collect::<Vec<usize>>();
            if projection == &current_projection {
                plan
            } else {
                let fields: Vec<Expr> = projection
                    .iter()
                    .map(|i| {
                        Expr::Column(Column::from(
                            self.df.logical_plan().schema().qualified_field(*i),
                        ))
                    })
                    .collect();
                plan.project(fields)?
            }
        } else {
            plan
        };

        if let Some(limit) = limit {
            plan = plan.limit(0, Some(limit))?;
        }

        state.create_physical_plan(&plan.build()?).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}

#[derive(Debug)]
pub(crate) struct RustWrappedPyTableProviderFactory {
    pub(crate) table_provider_factory: Py<PyAny>,
    pub(crate) codec: Arc<FFI_LogicalExtensionCodec>,
}

impl RustWrappedPyTableProviderFactory {
    pub fn new(table_provider_factory: Py<PyAny>, codec: Arc<FFI_LogicalExtensionCodec>) -> Self {
        Self {
            table_provider_factory,
            codec,
        }
    }

    fn create_inner(
        &self,
        cmd: CreateExternalTable,
        codec: Bound<PyAny>,
    ) -> PyResult<Arc<dyn TableProvider>> {
        Python::attach(|py| {
            let provider = self.table_provider_factory.bind(py);
            let cmd = PyCreateExternalTable::from(cmd);

            provider
                .call_method1("create", (cmd,))
                .and_then(|t| PyTable::new(t, Some(codec)))
                .map(|t| t.table())
        })
    }
}

#[async_trait]
impl TableProviderFactory for RustWrappedPyTableProviderFactory {
    async fn create(
        &self,
        _: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        Python::attach(|py| {
            let codec = create_logical_extension_capsule(py, self.codec.as_ref())
                .map_err(errors::to_datafusion_err)?;

            self.create_inner(cmd.clone(), codec.into_any())
                .map_err(errors::to_datafusion_err)
        })
    }
}
