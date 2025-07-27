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

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::RecordBatchReader;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::FromPyArrow;
use datafusion::execution::session_state::SessionStateBuilder;
use object_store::ObjectStore;
use url::Url;
use uuid::Uuid;

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;

use crate::catalog::{PyCatalog, PyTable, RustWrappedPyCatalogProvider};
use crate::dataframe::PyDataFrame;
use crate::dataset::Dataset;
use crate::errors::{py_datafusion_err, to_datafusion_err, PyDataFusionResult};
use crate::expr::sort_expr::PySortExpr;
use crate::physical_plan::PyExecutionPlan;
use crate::record_batch::PyRecordBatchStream;
use crate::sql::exceptions::py_value_err;
use crate::sql::logical::PyLogicalPlan;
use crate::store::StorageContexts;
use crate::udaf::PyAggregateUDF;
use crate::udf::PyScalarUDF;
use crate::udtf::PyTableFunction;
use crate::udwf::PyWindowUDF;
use crate::utils::{get_global_ctx, get_tokio_runtime, validate_pycapsule, wait_for_future};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::CatalogProvider;
use datafusion::common::TableReference;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::{
    DataFilePaths, SQLOptions, SessionConfig, SessionContext, TaskContext,
};
use datafusion::execution::disk_manager::DiskManagerMode;
use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool, UnboundedMemoryPool};
use datafusion::execution::options::ReadOptions;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::{
    AvroReadOptions, CsvReadOptions, DataFrame, NdJsonReadOptions, ParquetReadOptions,
};
use datafusion_ffi::catalog_provider::{FFI_CatalogProvider, ForeignCatalogProvider};
use datafusion_ffi::table_provider::{FFI_TableProvider, ForeignTableProvider};
use pyo3::types::{PyCapsule, PyDict, PyList, PyTuple, PyType};
use pyo3::IntoPyObjectExt;
use tokio::task::JoinHandle;

/// Configuration options for a SessionContext
#[pyclass(name = "SessionConfig", module = "datafusion", subclass)]
#[derive(Clone, Default)]
pub struct PySessionConfig {
    pub config: SessionConfig,
}

impl From<SessionConfig> for PySessionConfig {
    fn from(config: SessionConfig) -> Self {
        Self { config }
    }
}

#[pymethods]
impl PySessionConfig {
    #[pyo3(signature = (config_options=None))]
    #[new]
    fn new(config_options: Option<HashMap<String, String>>) -> Self {
        let mut config = SessionConfig::new();
        if let Some(hash_map) = config_options {
            for (k, v) in &hash_map {
                config = config.set(k, &ScalarValue::Utf8(Some(v.clone())));
            }
        }

        Self { config }
    }

    fn with_create_default_catalog_and_schema(&self, enabled: bool) -> Self {
        Self::from(
            self.config
                .clone()
                .with_create_default_catalog_and_schema(enabled),
        )
    }

    fn with_default_catalog_and_schema(&self, catalog: &str, schema: &str) -> Self {
        Self::from(
            self.config
                .clone()
                .with_default_catalog_and_schema(catalog, schema),
        )
    }

    fn with_information_schema(&self, enabled: bool) -> Self {
        Self::from(self.config.clone().with_information_schema(enabled))
    }

    fn with_batch_size(&self, batch_size: usize) -> Self {
        Self::from(self.config.clone().with_batch_size(batch_size))
    }

    fn with_target_partitions(&self, target_partitions: usize) -> Self {
        Self::from(
            self.config
                .clone()
                .with_target_partitions(target_partitions),
        )
    }

    fn with_repartition_aggregations(&self, enabled: bool) -> Self {
        Self::from(self.config.clone().with_repartition_aggregations(enabled))
    }

    fn with_repartition_joins(&self, enabled: bool) -> Self {
        Self::from(self.config.clone().with_repartition_joins(enabled))
    }

    fn with_repartition_windows(&self, enabled: bool) -> Self {
        Self::from(self.config.clone().with_repartition_windows(enabled))
    }

    fn with_repartition_sorts(&self, enabled: bool) -> Self {
        Self::from(self.config.clone().with_repartition_sorts(enabled))
    }

    fn with_repartition_file_scans(&self, enabled: bool) -> Self {
        Self::from(self.config.clone().with_repartition_file_scans(enabled))
    }

    fn with_repartition_file_min_size(&self, size: usize) -> Self {
        Self::from(self.config.clone().with_repartition_file_min_size(size))
    }

    fn with_parquet_pruning(&self, enabled: bool) -> Self {
        Self::from(self.config.clone().with_parquet_pruning(enabled))
    }

    fn set(&self, key: &str, value: &str) -> Self {
        Self::from(self.config.clone().set_str(key, value))
    }
}

/// Runtime options for a SessionContext
#[pyclass(name = "RuntimeEnvBuilder", module = "datafusion", subclass)]
#[derive(Clone)]
pub struct PyRuntimeEnvBuilder {
    pub builder: RuntimeEnvBuilder,
}

#[pymethods]
impl PyRuntimeEnvBuilder {
    #[new]
    fn new() -> Self {
        Self {
            builder: RuntimeEnvBuilder::default(),
        }
    }

    fn with_disk_manager_disabled(&self) -> Self {
        let mut runtime_builder = self.builder.clone();

        let mut disk_mgr_builder = runtime_builder
            .disk_manager_builder
            .clone()
            .unwrap_or_default();
        disk_mgr_builder.set_mode(DiskManagerMode::Disabled);

        runtime_builder = runtime_builder.with_disk_manager_builder(disk_mgr_builder);
        Self {
            builder: runtime_builder,
        }
    }

    fn with_disk_manager_os(&self) -> Self {
        let mut runtime_builder = self.builder.clone();

        let mut disk_mgr_builder = runtime_builder
            .disk_manager_builder
            .clone()
            .unwrap_or_default();
        disk_mgr_builder.set_mode(DiskManagerMode::OsTmpDirectory);

        runtime_builder = runtime_builder.with_disk_manager_builder(disk_mgr_builder);
        Self {
            builder: runtime_builder,
        }
    }

    fn with_disk_manager_specified(&self, paths: Vec<String>) -> Self {
        let paths = paths.iter().map(|s| s.into()).collect();
        let mut runtime_builder = self.builder.clone();

        let mut disk_mgr_builder = runtime_builder
            .disk_manager_builder
            .clone()
            .unwrap_or_default();
        disk_mgr_builder.set_mode(DiskManagerMode::Directories(paths));

        runtime_builder = runtime_builder.with_disk_manager_builder(disk_mgr_builder);
        Self {
            builder: runtime_builder,
        }
    }

    fn with_unbounded_memory_pool(&self) -> Self {
        let builder = self.builder.clone();
        let builder = builder.with_memory_pool(Arc::new(UnboundedMemoryPool::default()));
        Self { builder }
    }

    fn with_fair_spill_pool(&self, size: usize) -> Self {
        let builder = self.builder.clone();
        let builder = builder.with_memory_pool(Arc::new(FairSpillPool::new(size)));
        Self { builder }
    }

    fn with_greedy_memory_pool(&self, size: usize) -> Self {
        let builder = self.builder.clone();
        let builder = builder.with_memory_pool(Arc::new(GreedyMemoryPool::new(size)));
        Self { builder }
    }

    fn with_temp_file_path(&self, path: &str) -> Self {
        let builder = self.builder.clone();
        let builder = builder.with_temp_file_path(path);
        Self { builder }
    }
}

/// `PySQLOptions` allows you to specify options to the sql execution.
#[pyclass(name = "SQLOptions", module = "datafusion", subclass)]
#[derive(Clone)]
pub struct PySQLOptions {
    pub options: SQLOptions,
}

impl From<SQLOptions> for PySQLOptions {
    fn from(options: SQLOptions) -> Self {
        Self { options }
    }
}

#[pymethods]
impl PySQLOptions {
    #[new]
    fn new() -> Self {
        let options = SQLOptions::new();
        Self { options }
    }

    /// Should DDL data modification commands  (e.g. `CREATE TABLE`) be run? Defaults to `true`.
    fn with_allow_ddl(&self, allow: bool) -> Self {
        Self::from(self.options.with_allow_ddl(allow))
    }

    /// Should DML data modification commands (e.g. `INSERT and COPY`) be run? Defaults to `true`
    pub fn with_allow_dml(&self, allow: bool) -> Self {
        Self::from(self.options.with_allow_dml(allow))
    }

    /// Should Statements such as (e.g. `SET VARIABLE and `BEGIN TRANSACTION` ...`) be run?. Defaults to `true`
    pub fn with_allow_statements(&self, allow: bool) -> Self {
        Self::from(self.options.with_allow_statements(allow))
    }
}

/// `PySessionContext` is able to plan and execute DataFusion plans.
/// It has a powerful optimizer, a physical planner for local execution, and a
/// multi-threaded execution engine to perform the execution.
#[pyclass(name = "SessionContext", module = "datafusion", subclass)]
#[derive(Clone)]
pub struct PySessionContext {
    pub ctx: SessionContext,
}

#[pymethods]
impl PySessionContext {
    #[pyo3(signature = (config=None, runtime=None))]
    #[new]
    pub fn new(
        config: Option<PySessionConfig>,
        runtime: Option<PyRuntimeEnvBuilder>,
    ) -> PyDataFusionResult<Self> {
        let config = if let Some(c) = config {
            c.config
        } else {
            SessionConfig::default().with_information_schema(true)
        };
        let runtime_env_builder = if let Some(c) = runtime {
            c.builder
        } else {
            RuntimeEnvBuilder::default()
        };
        let runtime = Arc::new(runtime_env_builder.build()?);
        let session_state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .build();
        Ok(PySessionContext {
            ctx: SessionContext::new_with_state(session_state),
        })
    }

    pub fn enable_url_table(&self) -> PyResult<Self> {
        Ok(PySessionContext {
            ctx: self.ctx.clone().enable_url_table(),
        })
    }

    #[classmethod]
    #[pyo3(signature = ())]
    fn global_ctx(_cls: &Bound<'_, PyType>) -> PyResult<Self> {
        Ok(Self {
            ctx: get_global_ctx().clone(),
        })
    }

    /// Register an object store with the given name
    #[pyo3(signature = (scheme, store, host=None))]
    pub fn register_object_store(
        &mut self,
        scheme: &str,
        store: StorageContexts,
        host: Option<&str>,
    ) -> PyResult<()> {
        // for most stores the "host" is the bucket name and can be inferred from the store
        let (store, upstream_host): (Arc<dyn ObjectStore>, String) = match store {
            StorageContexts::AmazonS3(s3) => (s3.inner, s3.bucket_name),
            StorageContexts::GoogleCloudStorage(gcs) => (gcs.inner, gcs.bucket_name),
            StorageContexts::MicrosoftAzure(azure) => (azure.inner, azure.container_name),
            StorageContexts::LocalFileSystem(local) => (local.inner, "".to_string()),
            StorageContexts::HTTP(http) => (http.store, http.url),
        };

        // let users override the host to match the api signature from upstream
        let derived_host = if let Some(host) = host {
            host
        } else {
            &upstream_host
        };
        let url_string = format!("{scheme}{derived_host}");
        let url = Url::parse(&url_string).unwrap();
        self.ctx.runtime_env().register_object_store(&url, store);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, path, table_partition_cols=vec![],
    file_extension=".parquet",
    schema=None,
    file_sort_order=None))]
    pub fn register_listing_table(
        &mut self,
        name: &str,
        path: &str,
        table_partition_cols: Vec<(String, PyArrowType<DataType>)>,
        file_extension: &str,
        schema: Option<PyArrowType<Schema>>,
        file_sort_order: Option<Vec<Vec<PySortExpr>>>,
        py: Python,
    ) -> PyDataFusionResult<()> {
        let options = ListingOptions::new(Arc::new(ParquetFormat::new()))
            .with_file_extension(file_extension)
            .with_table_partition_cols(
                table_partition_cols
                    .into_iter()
                    .map(|(name, ty)| (name, ty.0))
                    .collect::<Vec<(String, DataType)>>(),
            )
            .with_file_sort_order(
                file_sort_order
                    .unwrap_or_default()
                    .into_iter()
                    .map(|e| e.into_iter().map(|f| f.into()).collect())
                    .collect(),
            );
        let table_path = ListingTableUrl::parse(path)?;
        let resolved_schema: SchemaRef = match schema {
            Some(s) => Arc::new(s.0),
            None => {
                let state = self.ctx.state();
                let schema = options.infer_schema(&state, &table_path);
                wait_for_future(py, schema)??
            }
        };
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(resolved_schema);
        let table = ListingTable::try_new(config)?;
        self.register_table(
            name,
            &PyTable {
                table: Arc::new(table),
            },
        )?;
        Ok(())
    }

    pub fn register_udtf(&mut self, func: PyTableFunction) {
        let name = func.name.clone();
        let func = Arc::new(func);
        self.ctx.register_udtf(&name, func);
    }

    /// Returns a PyDataFrame whose plan corresponds to the SQL statement.
    pub fn sql(&mut self, query: &str, py: Python) -> PyDataFusionResult<PyDataFrame> {
        let result = self.ctx.sql(query);
        let df = wait_for_future(py, result)??;
        Ok(PyDataFrame::new(df))
    }

    #[pyo3(signature = (query, options=None))]
    pub fn sql_with_options(
        &mut self,
        query: &str,
        options: Option<PySQLOptions>,
        py: Python,
    ) -> PyDataFusionResult<PyDataFrame> {
        let options = if let Some(options) = options {
            options.options
        } else {
            SQLOptions::new()
        };
        let result = self.ctx.sql_with_options(query, options);
        let df = wait_for_future(py, result)??;
        Ok(PyDataFrame::new(df))
    }

    #[pyo3(signature = (partitions, name=None, schema=None))]
    pub fn create_dataframe(
        &mut self,
        partitions: PyArrowType<Vec<Vec<RecordBatch>>>,
        name: Option<&str>,
        schema: Option<PyArrowType<Schema>>,
        py: Python,
    ) -> PyDataFusionResult<PyDataFrame> {
        let schema = if let Some(schema) = schema {
            SchemaRef::from(schema.0)
        } else {
            partitions.0[0][0].schema()
        };

        let table = MemTable::try_new(schema, partitions.0)?;

        // generate a random (unique) name for this table if none is provided
        // table name cannot start with numeric digit
        let table_name = match name {
            Some(val) => val.to_owned(),
            None => {
                "c".to_owned()
                    + Uuid::new_v4()
                        .simple()
                        .encode_lower(&mut Uuid::encode_buffer())
            }
        };

        self.ctx.register_table(&*table_name, Arc::new(table))?;

        let table = wait_for_future(py, self._table(&table_name))??;

        let df = PyDataFrame::new(table);
        Ok(df)
    }

    /// Create a DataFrame from an existing logical plan
    pub fn create_dataframe_from_logical_plan(&mut self, plan: PyLogicalPlan) -> PyDataFrame {
        PyDataFrame::new(DataFrame::new(self.ctx.state(), plan.plan.as_ref().clone()))
    }

    /// Construct datafusion dataframe from Python list
    #[pyo3(signature = (data, name=None))]
    pub fn from_pylist(
        &mut self,
        data: Bound<'_, PyList>,
        name: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        // Acquire GIL Token
        let py = data.py();

        // Instantiate pyarrow Table object & convert to Arrow Table
        let table_class = py.import("pyarrow")?.getattr("Table")?;
        let args = PyTuple::new(py, &[data])?;
        let table = table_class.call_method1("from_pylist", args)?;

        // Convert Arrow Table to datafusion DataFrame
        let df = self.from_arrow(table, name, py)?;
        Ok(df)
    }

    /// Construct datafusion dataframe from Python dictionary
    #[pyo3(signature = (data, name=None))]
    pub fn from_pydict(
        &mut self,
        data: Bound<'_, PyDict>,
        name: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        // Acquire GIL Token
        let py = data.py();

        // Instantiate pyarrow Table object & convert to Arrow Table
        let table_class = py.import("pyarrow")?.getattr("Table")?;
        let args = PyTuple::new(py, &[data])?;
        let table = table_class.call_method1("from_pydict", args)?;

        // Convert Arrow Table to datafusion DataFrame
        let df = self.from_arrow(table, name, py)?;
        Ok(df)
    }

    /// Construct datafusion dataframe from Arrow Table
    #[pyo3(signature = (data, name=None))]
    pub fn from_arrow(
        &mut self,
        data: Bound<'_, PyAny>,
        name: Option<&str>,
        py: Python,
    ) -> PyDataFusionResult<PyDataFrame> {
        let (schema, batches) =
            if let Ok(stream_reader) = ArrowArrayStreamReader::from_pyarrow_bound(&data) {
                // Works for any object that implements __arrow_c_stream__ in pycapsule.

                let schema = stream_reader.schema().as_ref().to_owned();
                let batches = stream_reader
                    .collect::<Result<Vec<RecordBatch>, arrow::error::ArrowError>>()?;

                (schema, batches)
            } else if let Ok(array) = RecordBatch::from_pyarrow_bound(&data) {
                // While this says RecordBatch, it will work for any object that implements
                // __arrow_c_array__ and returns a StructArray.

                (array.schema().as_ref().to_owned(), vec![array])
            } else {
                return Err(crate::errors::PyDataFusionError::Common(
                    "Expected either a Arrow Array or Arrow Stream in from_arrow().".to_string(),
                ));
            };

        // Because create_dataframe() expects a vector of vectors of record batches
        // here we need to wrap the vector of record batches in an additional vector
        let list_of_batches = PyArrowType::from(vec![batches]);
        self.create_dataframe(list_of_batches, name, Some(schema.into()), py)
    }

    /// Construct datafusion dataframe from pandas
    #[allow(clippy::wrong_self_convention)]
    #[pyo3(signature = (data, name=None))]
    pub fn from_pandas(
        &mut self,
        data: Bound<'_, PyAny>,
        name: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        // Obtain GIL token
        let py = data.py();

        // Instantiate pyarrow Table object & convert to Arrow Table
        let table_class = py.import("pyarrow")?.getattr("Table")?;
        let args = PyTuple::new(py, &[data])?;
        let table = table_class.call_method1("from_pandas", args)?;

        // Convert Arrow Table to datafusion DataFrame
        let df = self.from_arrow(table, name, py)?;
        Ok(df)
    }

    /// Construct datafusion dataframe from polars
    #[pyo3(signature = (data, name=None))]
    pub fn from_polars(
        &mut self,
        data: Bound<'_, PyAny>,
        name: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        // Convert Polars dataframe to Arrow Table
        let table = data.call_method0("to_arrow")?;

        // Convert Arrow Table to datafusion DataFrame
        let df = self.from_arrow(table, name, data.py())?;
        Ok(df)
    }

    pub fn register_table(&mut self, name: &str, table: &PyTable) -> PyDataFusionResult<()> {
        self.ctx.register_table(name, table.table())?;
        Ok(())
    }

    pub fn deregister_table(&mut self, name: &str) -> PyDataFusionResult<()> {
        self.ctx.deregister_table(name)?;
        Ok(())
    }

    pub fn register_catalog_provider(
        &mut self,
        name: &str,
        provider: Bound<'_, PyAny>,
    ) -> PyDataFusionResult<()> {
        let provider = if provider.hasattr("__datafusion_catalog_provider__")? {
            let capsule = provider
                .getattr("__datafusion_catalog_provider__")?
                .call0()?;
            let capsule = capsule.downcast::<PyCapsule>().map_err(py_datafusion_err)?;
            validate_pycapsule(capsule, "datafusion_catalog_provider")?;

            let provider = unsafe { capsule.reference::<FFI_CatalogProvider>() };
            let provider: ForeignCatalogProvider = provider.into();
            Arc::new(provider) as Arc<dyn CatalogProvider>
        } else {
            match provider.extract::<PyCatalog>() {
                Ok(py_catalog) => py_catalog.catalog,
                Err(_) => Arc::new(RustWrappedPyCatalogProvider::new(provider.into()))
                    as Arc<dyn CatalogProvider>,
            }
        };

        let _ = self.ctx.register_catalog(name, provider);

        Ok(())
    }

    /// Construct datafusion dataframe from Arrow Table
    pub fn register_table_provider(
        &mut self,
        name: &str,
        provider: Bound<'_, PyAny>,
    ) -> PyDataFusionResult<()> {
        if provider.hasattr("__datafusion_table_provider__")? {
            let capsule = provider.getattr("__datafusion_table_provider__")?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>().map_err(py_datafusion_err)?;
            validate_pycapsule(capsule, "datafusion_table_provider")?;

            let provider = unsafe { capsule.reference::<FFI_TableProvider>() };
            let provider: ForeignTableProvider = provider.into();

            let _ = self.ctx.register_table(name, Arc::new(provider))?;

            Ok(())
        } else {
            Err(crate::errors::PyDataFusionError::Common(
                "__datafusion_table_provider__ does not exist on Table Provider object."
                    .to_string(),
            ))
        }
    }

    pub fn register_record_batches(
        &mut self,
        name: &str,
        partitions: PyArrowType<Vec<Vec<RecordBatch>>>,
    ) -> PyDataFusionResult<()> {
        let schema = partitions.0[0][0].schema();
        let table = MemTable::try_new(schema, partitions.0)?;
        self.ctx.register_table(name, Arc::new(table))?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, path, table_partition_cols=vec![],
                        parquet_pruning=true,
                        file_extension=".parquet",
                        skip_metadata=true,
                        schema=None,
                        file_sort_order=None))]
    pub fn register_parquet(
        &mut self,
        name: &str,
        path: &str,
        table_partition_cols: Vec<(String, PyArrowType<DataType>)>,
        parquet_pruning: bool,
        file_extension: &str,
        skip_metadata: bool,
        schema: Option<PyArrowType<Schema>>,
        file_sort_order: Option<Vec<Vec<PySortExpr>>>,
        py: Python,
    ) -> PyDataFusionResult<()> {
        let mut options = ParquetReadOptions::default()
            .table_partition_cols(
                table_partition_cols
                    .into_iter()
                    .map(|(name, ty)| (name, ty.0))
                    .collect::<Vec<(String, DataType)>>(),
            )
            .parquet_pruning(parquet_pruning)
            .skip_metadata(skip_metadata);
        options.file_extension = file_extension;
        options.schema = schema.as_ref().map(|x| &x.0);
        options.file_sort_order = file_sort_order
            .unwrap_or_default()
            .into_iter()
            .map(|e| e.into_iter().map(|f| f.into()).collect())
            .collect();

        let result = self.ctx.register_parquet(name, path, options);
        wait_for_future(py, result)??;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name,
                        path,
                        schema=None,
                        has_header=true,
                        delimiter=",",
                        schema_infer_max_records=1000,
                        file_extension=".csv",
                        file_compression_type=None))]
    pub fn register_csv(
        &mut self,
        name: &str,
        path: &Bound<'_, PyAny>,
        schema: Option<PyArrowType<Schema>>,
        has_header: bool,
        delimiter: &str,
        schema_infer_max_records: usize,
        file_extension: &str,
        file_compression_type: Option<String>,
        py: Python,
    ) -> PyDataFusionResult<()> {
        let delimiter = delimiter.as_bytes();
        if delimiter.len() != 1 {
            return Err(crate::errors::PyDataFusionError::PythonError(py_value_err(
                "Delimiter must be a single character",
            )));
        }

        let mut options = CsvReadOptions::new()
            .has_header(has_header)
            .delimiter(delimiter[0])
            .schema_infer_max_records(schema_infer_max_records)
            .file_extension(file_extension)
            .file_compression_type(parse_file_compression_type(file_compression_type)?);
        options.schema = schema.as_ref().map(|x| &x.0);

        if path.is_instance_of::<PyList>() {
            let paths = path.extract::<Vec<String>>()?;
            let result = self.register_csv_from_multiple_paths(name, paths, options);
            wait_for_future(py, result)??;
        } else {
            let path = path.extract::<String>()?;
            let result = self.ctx.register_csv(name, &path, options);
            wait_for_future(py, result)??;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name,
                        path,
                        schema=None,
                        schema_infer_max_records=1000,
                        file_extension=".json",
                        table_partition_cols=vec![],
                        file_compression_type=None))]
    pub fn register_json(
        &mut self,
        name: &str,
        path: PathBuf,
        schema: Option<PyArrowType<Schema>>,
        schema_infer_max_records: usize,
        file_extension: &str,
        table_partition_cols: Vec<(String, PyArrowType<DataType>)>,
        file_compression_type: Option<String>,
        py: Python,
    ) -> PyDataFusionResult<()> {
        let path = path
            .to_str()
            .ok_or_else(|| PyValueError::new_err("Unable to convert path to a string"))?;

        let mut options = NdJsonReadOptions::default()
            .file_compression_type(parse_file_compression_type(file_compression_type)?)
            .table_partition_cols(
                table_partition_cols
                    .into_iter()
                    .map(|(name, ty)| (name, ty.0))
                    .collect::<Vec<(String, DataType)>>(),
            );
        options.schema_infer_max_records = schema_infer_max_records;
        options.file_extension = file_extension;
        options.schema = schema.as_ref().map(|x| &x.0);

        let result = self.ctx.register_json(name, path, options);
        wait_for_future(py, result)??;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name,
                        path,
                        schema=None,
                        file_extension=".avro",
                        table_partition_cols=vec![]))]
    pub fn register_avro(
        &mut self,
        name: &str,
        path: PathBuf,
        schema: Option<PyArrowType<Schema>>,
        file_extension: &str,
        table_partition_cols: Vec<(String, PyArrowType<DataType>)>,
        py: Python,
    ) -> PyDataFusionResult<()> {
        let path = path
            .to_str()
            .ok_or_else(|| PyValueError::new_err("Unable to convert path to a string"))?;

        let mut options = AvroReadOptions::default().table_partition_cols(
            table_partition_cols
                .into_iter()
                .map(|(name, ty)| (name, ty.0))
                .collect::<Vec<(String, DataType)>>(),
        );
        options.file_extension = file_extension;
        options.schema = schema.as_ref().map(|x| &x.0);

        let result = self.ctx.register_avro(name, path, options);
        wait_for_future(py, result)??;

        Ok(())
    }

    // Registers a PyArrow.Dataset
    pub fn register_dataset(
        &self,
        name: &str,
        dataset: &Bound<'_, PyAny>,
        py: Python,
    ) -> PyDataFusionResult<()> {
        let table: Arc<dyn TableProvider> = Arc::new(Dataset::new(dataset, py)?);

        self.ctx.register_table(name, table)?;

        Ok(())
    }

    pub fn register_udf(&mut self, udf: PyScalarUDF) -> PyResult<()> {
        self.ctx.register_udf(udf.function);
        Ok(())
    }

    pub fn register_udaf(&mut self, udaf: PyAggregateUDF) -> PyResult<()> {
        self.ctx.register_udaf(udaf.function);
        Ok(())
    }

    pub fn register_udwf(&mut self, udwf: PyWindowUDF) -> PyResult<()> {
        self.ctx.register_udwf(udwf.function);
        Ok(())
    }

    #[pyo3(signature = (name="datafusion"))]
    pub fn catalog(&self, name: &str) -> PyResult<PyObject> {
        let catalog = self.ctx.catalog(name).ok_or(PyKeyError::new_err(format!(
            "Catalog with name {name} doesn't exist."
        )))?;

        Python::with_gil(|py| {
            match catalog
                .as_any()
                .downcast_ref::<RustWrappedPyCatalogProvider>()
            {
                Some(wrapped_schema) => Ok(wrapped_schema.catalog_provider.clone_ref(py)),
                None => PyCatalog::from(catalog).into_py_any(py),
            }
        })
    }

    pub fn catalog_names(&self) -> HashSet<String> {
        self.ctx.catalog_names().into_iter().collect()
    }

    pub fn tables(&self) -> HashSet<String> {
        self.ctx
            .catalog_names()
            .into_iter()
            .filter_map(|name| self.ctx.catalog(&name))
            .flat_map(move |catalog| {
                catalog
                    .schema_names()
                    .into_iter()
                    .filter_map(move |name| catalog.schema(&name))
            })
            .flat_map(|schema| schema.table_names())
            .collect()
    }

    pub fn table(&self, name: &str, py: Python) -> PyResult<PyDataFrame> {
        let res = wait_for_future(py, self.ctx.table(name))
            .map_err(|e| PyKeyError::new_err(e.to_string()))?;
        match res {
            Ok(df) => Ok(PyDataFrame::new(df)),
            Err(e) => {
                if let datafusion::error::DataFusionError::Plan(msg) = &e {
                    if msg.contains("No table named") {
                        return Err(PyKeyError::new_err(msg.to_string()));
                    }
                }
                Err(py_datafusion_err(e))
            }
        }
    }

    pub fn table_exist(&self, name: &str) -> PyDataFusionResult<bool> {
        Ok(self.ctx.table_exist(name)?)
    }

    pub fn empty_table(&self) -> PyDataFusionResult<PyDataFrame> {
        Ok(PyDataFrame::new(self.ctx.read_empty()?))
    }

    pub fn session_id(&self) -> String {
        self.ctx.session_id()
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (path, schema=None, schema_infer_max_records=1000, file_extension=".json", table_partition_cols=vec![], file_compression_type=None))]
    pub fn read_json(
        &mut self,
        path: PathBuf,
        schema: Option<PyArrowType<Schema>>,
        schema_infer_max_records: usize,
        file_extension: &str,
        table_partition_cols: Vec<(String, PyArrowType<DataType>)>,
        file_compression_type: Option<String>,
        py: Python,
    ) -> PyDataFusionResult<PyDataFrame> {
        let path = path
            .to_str()
            .ok_or_else(|| PyValueError::new_err("Unable to convert path to a string"))?;
        let mut options = NdJsonReadOptions::default()
            .table_partition_cols(
                table_partition_cols
                    .into_iter()
                    .map(|(name, ty)| (name, ty.0))
                    .collect::<Vec<(String, DataType)>>(),
            )
            .file_compression_type(parse_file_compression_type(file_compression_type)?);
        options.schema_infer_max_records = schema_infer_max_records;
        options.file_extension = file_extension;
        let df = if let Some(schema) = schema {
            options.schema = Some(&schema.0);
            let result = self.ctx.read_json(path, options);
            wait_for_future(py, result)??
        } else {
            let result = self.ctx.read_json(path, options);
            wait_for_future(py, result)??
        };
        Ok(PyDataFrame::new(df))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        path,
        schema=None,
        has_header=true,
        delimiter=",",
        schema_infer_max_records=1000,
        file_extension=".csv",
        table_partition_cols=vec![],
        file_compression_type=None))]
    pub fn read_csv(
        &self,
        path: &Bound<'_, PyAny>,
        schema: Option<PyArrowType<Schema>>,
        has_header: bool,
        delimiter: &str,
        schema_infer_max_records: usize,
        file_extension: &str,
        table_partition_cols: Vec<(String, PyArrowType<DataType>)>,
        file_compression_type: Option<String>,
        py: Python,
    ) -> PyDataFusionResult<PyDataFrame> {
        let delimiter = delimiter.as_bytes();
        if delimiter.len() != 1 {
            return Err(crate::errors::PyDataFusionError::PythonError(py_value_err(
                "Delimiter must be a single character",
            )));
        };

        let mut options = CsvReadOptions::new()
            .has_header(has_header)
            .delimiter(delimiter[0])
            .schema_infer_max_records(schema_infer_max_records)
            .file_extension(file_extension)
            .table_partition_cols(
                table_partition_cols
                    .into_iter()
                    .map(|(name, ty)| (name, ty.0))
                    .collect::<Vec<(String, DataType)>>(),
            )
            .file_compression_type(parse_file_compression_type(file_compression_type)?);
        options.schema = schema.as_ref().map(|x| &x.0);

        if path.is_instance_of::<PyList>() {
            let paths = path.extract::<Vec<String>>()?;
            let paths = paths.iter().map(|p| p as &str).collect::<Vec<&str>>();
            let result = self.ctx.read_csv(paths, options);
            let df = PyDataFrame::new(wait_for_future(py, result)??);
            Ok(df)
        } else {
            let path = path.extract::<String>()?;
            let result = self.ctx.read_csv(path, options);
            let df = PyDataFrame::new(wait_for_future(py, result)??);
            Ok(df)
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        path,
        table_partition_cols=vec![],
        parquet_pruning=true,
        file_extension=".parquet",
        skip_metadata=true,
        schema=None,
        file_sort_order=None))]
    pub fn read_parquet(
        &self,
        path: &str,
        table_partition_cols: Vec<(String, PyArrowType<DataType>)>,
        parquet_pruning: bool,
        file_extension: &str,
        skip_metadata: bool,
        schema: Option<PyArrowType<Schema>>,
        file_sort_order: Option<Vec<Vec<PySortExpr>>>,
        py: Python,
    ) -> PyDataFusionResult<PyDataFrame> {
        let mut options = ParquetReadOptions::default()
            .table_partition_cols(
                table_partition_cols
                    .into_iter()
                    .map(|(name, ty)| (name, ty.0))
                    .collect::<Vec<(String, DataType)>>(),
            )
            .parquet_pruning(parquet_pruning)
            .skip_metadata(skip_metadata);
        options.file_extension = file_extension;
        options.schema = schema.as_ref().map(|x| &x.0);
        options.file_sort_order = file_sort_order
            .unwrap_or_default()
            .into_iter()
            .map(|e| e.into_iter().map(|f| f.into()).collect())
            .collect();

        let result = self.ctx.read_parquet(path, options);
        let df = PyDataFrame::new(wait_for_future(py, result)??);
        Ok(df)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (path, schema=None, table_partition_cols=vec![], file_extension=".avro"))]
    pub fn read_avro(
        &self,
        path: &str,
        schema: Option<PyArrowType<Schema>>,
        table_partition_cols: Vec<(String, PyArrowType<DataType>)>,
        file_extension: &str,
        py: Python,
    ) -> PyDataFusionResult<PyDataFrame> {
        let mut options = AvroReadOptions::default().table_partition_cols(
            table_partition_cols
                .into_iter()
                .map(|(name, ty)| (name, ty.0))
                .collect::<Vec<(String, DataType)>>(),
        );
        options.file_extension = file_extension;
        let df = if let Some(schema) = schema {
            options.schema = Some(&schema.0);
            let read_future = self.ctx.read_avro(path, options);
            wait_for_future(py, read_future)??
        } else {
            let read_future = self.ctx.read_avro(path, options);
            wait_for_future(py, read_future)??
        };
        Ok(PyDataFrame::new(df))
    }

    pub fn read_table(&self, table: &PyTable) -> PyDataFusionResult<PyDataFrame> {
        let df = self.ctx.read_table(table.table())?;
        Ok(PyDataFrame::new(df))
    }

    fn __repr__(&self) -> PyResult<String> {
        let config = self.ctx.copied_config();
        let mut config_entries = config
            .options()
            .entries()
            .iter()
            .filter(|e| e.value.is_some())
            .map(|e| format!("{} = {}", e.key, e.value.as_ref().unwrap()))
            .collect::<Vec<_>>();
        config_entries.sort();
        Ok(format!(
            "SessionContext: id={}; configs=[\n\t{}]",
            self.session_id(),
            config_entries.join("\n\t")
        ))
    }

    /// Execute a partition of an execution plan and return a stream of record batches
    pub fn execute(
        &self,
        plan: PyExecutionPlan,
        part: usize,
        py: Python,
    ) -> PyDataFusionResult<PyRecordBatchStream> {
        let ctx: TaskContext = TaskContext::from(&self.ctx.state());
        // create a Tokio runtime to run the async code
        let rt = &get_tokio_runtime().0;
        let plan = plan.plan.clone();
        let fut: JoinHandle<datafusion::common::Result<SendableRecordBatchStream>> =
            rt.spawn(async move { plan.execute(part, Arc::new(ctx)) });
        let stream = wait_for_future(py, async { fut.await.map_err(to_datafusion_err) })???;
        Ok(PyRecordBatchStream::new(stream))
    }
}

impl PySessionContext {
    async fn _table(&self, name: &str) -> datafusion::common::Result<DataFrame> {
        self.ctx.table(name).await
    }

    async fn register_csv_from_multiple_paths(
        &self,
        name: &str,
        table_paths: Vec<String>,
        options: CsvReadOptions<'_>,
    ) -> datafusion::common::Result<()> {
        let table_paths = table_paths.to_urls()?;
        let session_config = self.ctx.copied_config();
        let listing_options =
            options.to_listing_options(&session_config, self.ctx.copied_table_options());

        let option_extension = listing_options.file_extension.clone();

        if table_paths.is_empty() {
            return exec_err!("No table paths were provided");
        }

        // check if the file extension matches the expected extension
        for path in &table_paths {
            let file_path = path.as_str();
            if !file_path.ends_with(option_extension.clone().as_str()) && !path.is_collection() {
                return exec_err!(
                    "File path '{file_path}' does not match the expected extension '{option_extension}'"
                );
            }
        }

        let resolved_schema = options
            .get_resolved_schema(&session_config, self.ctx.state(), table_paths[0].clone())
            .await?;

        let config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
        let table = ListingTable::try_new(config)?;
        self.ctx
            .register_table(TableReference::Bare { table: name.into() }, Arc::new(table))?;
        Ok(())
    }
}

pub fn parse_file_compression_type(
    file_compression_type: Option<String>,
) -> Result<FileCompressionType, PyErr> {
    FileCompressionType::from_str(&*file_compression_type.unwrap_or("".to_string()).as_str())
        .map_err(|_| {
            PyValueError::new_err("file_compression_type must one of: gzip, bz2, xz, zstd")
        })
}

impl From<PySessionContext> for SessionContext {
    fn from(ctx: PySessionContext) -> SessionContext {
        ctx.ctx
    }
}

impl From<SessionContext> for PySessionContext {
    fn from(ctx: SessionContext) -> PySessionContext {
        PySessionContext { ctx }
    }
}
