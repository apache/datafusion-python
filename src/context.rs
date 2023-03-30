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
use std::sync::Arc;

use object_store::ObjectStore;
use url::Url;
use uuid::Uuid;

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;

use crate::catalog::{PyCatalog, PyTable};
use crate::dataframe::PyDataFrame;
use crate::dataset::Dataset;
use crate::errors::{py_datafusion_err, DataFusionError};
use crate::physical_plan::PyExecutionPlan;
use crate::record_batch::PyRecordBatchStream;
use crate::sql::logical::PyLogicalPlan;
use crate::store::StorageContexts;
use crate::udaf::PyAggregateUDF;
use crate::udf::PyScalarUDF;
use crate::utils::wait_for_future;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::datasource::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::execution::context::{SessionConfig, SessionContext, TaskContext};
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool, UnboundedMemoryPool};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::{
    AvroReadOptions, CsvReadOptions, DataFrame, NdJsonReadOptions, ParquetReadOptions,
};
use datafusion_common::ScalarValue;
use pyo3::types::PyTuple;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

#[pyclass(name = "SessionConfig", module = "datafusion", subclass, unsendable)]
#[derive(Clone, Default)]
pub(crate) struct PySessionConfig {
    pub(crate) config: SessionConfig,
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
                config = config.set(k, ScalarValue::Utf8(Some(v.clone())));
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
}

#[pyclass(name = "RuntimeConfig", module = "datafusion", subclass, unsendable)]
#[derive(Clone)]
pub(crate) struct PyRuntimeConfig {
    pub(crate) config: RuntimeConfig,
}

#[pymethods]
impl PyRuntimeConfig {
    #[new]
    fn new() -> Self {
        Self {
            config: RuntimeConfig::default(),
        }
    }

    fn with_disk_manager_disabled(&self) -> Self {
        let config = self.config.clone();
        let config = config.with_disk_manager(DiskManagerConfig::Disabled);
        Self { config }
    }

    fn with_disk_manager_os(&self) -> Self {
        let config = self.config.clone();
        let config = config.with_disk_manager(DiskManagerConfig::NewOs);
        Self { config }
    }

    fn with_disk_manager_specified(&self, paths: Vec<String>) -> Self {
        let config = self.config.clone();
        let paths = paths.iter().map(|s| s.into()).collect();
        let config = config.with_disk_manager(DiskManagerConfig::NewSpecified(paths));
        Self { config }
    }

    fn with_unbounded_memory_pool(&self) -> Self {
        let config = self.config.clone();
        let config = config.with_memory_pool(Arc::new(UnboundedMemoryPool::default()));
        Self { config }
    }

    fn with_fair_spill_pool(&self, size: usize) -> Self {
        let config = self.config.clone();
        let config = config.with_memory_pool(Arc::new(FairSpillPool::new(size)));
        Self { config }
    }

    fn with_greedy_memory_pool(&self, size: usize) -> Self {
        let config = self.config.clone();
        let config = config.with_memory_pool(Arc::new(GreedyMemoryPool::new(size)));
        Self { config }
    }

    fn with_temp_file_path(&self, path: &str) -> Self {
        let config = self.config.clone();
        let config = config.with_temp_file_path(path);
        Self { config }
    }
}

/// `PySessionContext` is able to plan and execute DataFusion plans.
/// It has a powerful optimizer, a physical planner for local execution, and a
/// multi-threaded execution engine to perform the execution.
#[pyclass(name = "SessionContext", module = "datafusion", subclass, unsendable)]
#[derive(Clone)]
pub(crate) struct PySessionContext {
    pub(crate) ctx: SessionContext,
}

#[pymethods]
impl PySessionContext {
    #[pyo3(signature = (config=None, runtime=None))]
    #[new]
    fn new(config: Option<PySessionConfig>, runtime: Option<PyRuntimeConfig>) -> PyResult<Self> {
        let config = if let Some(c) = config {
            c.config
        } else {
            SessionConfig::default()
        };
        let runtime_config = if let Some(c) = runtime {
            c.config
        } else {
            RuntimeConfig::default()
        };
        let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
        Ok(PySessionContext {
            ctx: SessionContext::with_config_rt(config, runtime),
        })
    }

    /// Register a an object store with the given name
    fn register_object_store(
        &mut self,
        scheme: &str,
        store: &PyAny,
        host: Option<&str>,
    ) -> PyResult<()> {
        let res: Result<(Arc<dyn ObjectStore>, String), PyErr> =
            match StorageContexts::extract(store) {
                Ok(store) => match store {
                    StorageContexts::AmazonS3(s3) => Ok((s3.inner, s3.bucket_name)),
                    StorageContexts::GoogleCloudStorage(gcs) => Ok((gcs.inner, gcs.bucket_name)),
                    StorageContexts::MicrosoftAzure(azure) => {
                        Ok((azure.inner, azure.container_name))
                    }
                    StorageContexts::LocalFileSystem(local) => Ok((local.inner, "".to_string())),
                },
                Err(_e) => Err(PyValueError::new_err("Invalid object store")),
            };

        // for most stores the "host" is the bucket name and can be inferred from the store
        let (store, upstream_host) = res?;
        // let users override the host to match the api signature from upstream
        let derived_host = if let Some(host) = host {
            host
        } else {
            &upstream_host
        };
        let url_string = format!("{}{}", scheme, derived_host);
        let url = Url::parse(&url_string).unwrap();
        self.ctx.runtime_env().register_object_store(&url, store);
        Ok(())
    }

    /// Returns a PyDataFrame whose plan corresponds to the SQL statement.
    fn sql(&mut self, query: &str, py: Python) -> PyResult<PyDataFrame> {
        let result = self.ctx.sql(query);
        let df = wait_for_future(py, result).map_err(DataFusionError::from)?;
        Ok(PyDataFrame::new(df))
    }

    fn create_dataframe(
        &mut self,
        partitions: PyArrowType<Vec<Vec<RecordBatch>>>,
        name: Option<&str>,
        py: Python,
    ) -> PyResult<PyDataFrame> {
        let schema = partitions.0[0][0].schema();
        let table = MemTable::try_new(schema, partitions.0).map_err(DataFusionError::from)?;

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

        self.ctx
            .register_table(&*table_name, Arc::new(table))
            .map_err(DataFusionError::from)?;

        let table = wait_for_future(py, self._table(&table_name)).map_err(DataFusionError::from)?;

        let df = PyDataFrame::new(table);
        Ok(df)
    }

    /// Create a DataFrame from an existing logical plan
    fn create_dataframe_from_logical_plan(&mut self, plan: PyLogicalPlan) -> PyDataFrame {
        PyDataFrame::new(DataFrame::new(self.ctx.state(), plan.plan.as_ref().clone()))
    }

    /// Construct datafusion dataframe from Python list
    #[allow(clippy::wrong_self_convention)]
    fn from_pylist(
        &mut self,
        data: PyObject,
        name: Option<&str>,
        _py: Python,
    ) -> PyResult<PyDataFrame> {
        Python::with_gil(|py| {
            // Instantiate pyarrow Table object & convert to Arrow Table
            let table_class = py.import("pyarrow")?.getattr("Table")?;
            let args = PyTuple::new(py, &[data]);
            let table = table_class.call_method1("from_pylist", args)?.into();

            // Convert Arrow Table to datafusion DataFrame
            let df = self.from_arrow_table(table, name, py)?;
            Ok(df)
        })
    }

    /// Construct datafusion dataframe from Python dictionary
    #[allow(clippy::wrong_self_convention)]
    fn from_pydict(
        &mut self,
        data: PyObject,
        name: Option<&str>,
        _py: Python,
    ) -> PyResult<PyDataFrame> {
        Python::with_gil(|py| {
            // Instantiate pyarrow Table object & convert to Arrow Table
            let table_class = py.import("pyarrow")?.getattr("Table")?;
            let args = PyTuple::new(py, &[data]);
            let table = table_class.call_method1("from_pydict", args)?.into();

            // Convert Arrow Table to datafusion DataFrame
            let df = self.from_arrow_table(table, name, py)?;
            Ok(df)
        })
    }

    /// Construct datafusion dataframe from Arrow Table
    #[allow(clippy::wrong_self_convention)]
    fn from_arrow_table(
        &mut self,
        data: PyObject,
        name: Option<&str>,
        _py: Python,
    ) -> PyResult<PyDataFrame> {
        Python::with_gil(|py| {
            // Instantiate pyarrow Table object & convert to batches
            let table = data.call_method0(py, "to_batches")?;

            // Cast PyObject to RecordBatch type
            // Because create_dataframe() expects a vector of vectors of record batches
            // here we need to wrap the vector of record batches in an additional vector
            let batches = table.extract::<PyArrowType<Vec<RecordBatch>>>(py)?;
            let list_of_batches = PyArrowType::try_from(vec![batches.0])?;
            self.create_dataframe(list_of_batches, name, py)
        })
    }

    /// Construct datafusion dataframe from pandas
    #[allow(clippy::wrong_self_convention)]
    fn from_pandas(
        &mut self,
        data: PyObject,
        name: Option<&str>,
        _py: Python,
    ) -> PyResult<PyDataFrame> {
        Python::with_gil(|py| {
            // Instantiate pyarrow Table object & convert to Arrow Table
            let table_class = py.import("pyarrow")?.getattr("Table")?;
            let args = PyTuple::new(py, &[data]);
            let table = table_class.call_method1("from_pandas", args)?.into();

            // Convert Arrow Table to datafusion DataFrame
            let df = self.from_arrow_table(table, name, py)?;
            Ok(df)
        })
    }

    /// Construct datafusion dataframe from polars
    #[allow(clippy::wrong_self_convention)]
    fn from_polars(
        &mut self,
        data: PyObject,
        name: Option<&str>,
        _py: Python,
    ) -> PyResult<PyDataFrame> {
        Python::with_gil(|py| {
            // Convert Polars dataframe to Arrow Table
            let table = data.call_method0(py, "to_arrow")?;

            // Convert Arrow Table to datafusion DataFrame
            let df = self.from_arrow_table(table, name, py)?;
            Ok(df)
        })
    }

    fn register_table(&mut self, name: &str, table: &PyTable) -> PyResult<()> {
        self.ctx
            .register_table(name, table.table())
            .map_err(DataFusionError::from)?;
        Ok(())
    }

    fn deregister_table(&mut self, name: &str) -> PyResult<()> {
        self.ctx
            .deregister_table(name)
            .map_err(DataFusionError::from)?;
        Ok(())
    }

    fn register_record_batches(
        &mut self,
        name: &str,
        partitions: PyArrowType<Vec<Vec<RecordBatch>>>,
    ) -> PyResult<()> {
        let schema = partitions.0[0][0].schema();
        let table = MemTable::try_new(schema, partitions.0)?;
        self.ctx
            .register_table(name, Arc::new(table))
            .map_err(DataFusionError::from)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, path, table_partition_cols=vec![],
                        parquet_pruning=true,
                        file_extension=".parquet"))]
    fn register_parquet(
        &mut self,
        name: &str,
        path: &str,
        table_partition_cols: Vec<(String, String)>,
        parquet_pruning: bool,
        file_extension: &str,
        py: Python,
    ) -> PyResult<()> {
        let mut options = ParquetReadOptions::default()
            .table_partition_cols(convert_table_partition_cols(table_partition_cols)?)
            .parquet_pruning(parquet_pruning);
        options.file_extension = file_extension;
        let result = self.ctx.register_parquet(name, path, options);
        wait_for_future(py, result).map_err(DataFusionError::from)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name,
                        path,
                        schema=None,
                        has_header=true,
                        delimiter=",",
                        schema_infer_max_records=1000,
                        file_extension=".csv"))]
    fn register_csv(
        &mut self,
        name: &str,
        path: PathBuf,
        schema: Option<PyArrowType<Schema>>,
        has_header: bool,
        delimiter: &str,
        schema_infer_max_records: usize,
        file_extension: &str,
        py: Python,
    ) -> PyResult<()> {
        let path = path
            .to_str()
            .ok_or_else(|| PyValueError::new_err("Unable to convert path to a string"))?;
        let delimiter = delimiter.as_bytes();
        if delimiter.len() != 1 {
            return Err(PyValueError::new_err(
                "Delimiter must be a single character",
            ));
        }

        let mut options = CsvReadOptions::new()
            .has_header(has_header)
            .delimiter(delimiter[0])
            .schema_infer_max_records(schema_infer_max_records)
            .file_extension(file_extension);
        options.schema = schema.as_ref().map(|x| &x.0);

        let result = self.ctx.register_csv(name, path, options);
        wait_for_future(py, result).map_err(DataFusionError::from)?;

        Ok(())
    }

    // Registers a PyArrow.Dataset
    fn register_dataset(&self, name: &str, dataset: &PyAny, py: Python) -> PyResult<()> {
        let table: Arc<dyn TableProvider> = Arc::new(Dataset::new(dataset, py)?);

        self.ctx
            .register_table(name, table)
            .map_err(DataFusionError::from)?;

        Ok(())
    }

    fn register_udf(&mut self, udf: PyScalarUDF) -> PyResult<()> {
        self.ctx.register_udf(udf.function);
        Ok(())
    }

    fn register_udaf(&mut self, udaf: PyAggregateUDF) -> PyResult<()> {
        self.ctx.register_udaf(udaf.function);
        Ok(())
    }

    #[pyo3(signature = (name="datafusion"))]
    fn catalog(&self, name: &str) -> PyResult<PyCatalog> {
        match self.ctx.catalog(name) {
            Some(catalog) => Ok(PyCatalog::new(catalog)),
            None => Err(PyKeyError::new_err(format!(
                "Catalog with name {} doesn't exist.",
                &name,
            ))),
        }
    }

    fn tables(&self) -> HashSet<String> {
        #[allow(deprecated)]
        self.ctx.tables().unwrap()
    }

    fn table(&self, name: &str, py: Python) -> PyResult<PyDataFrame> {
        let x = wait_for_future(py, self.ctx.table(name)).map_err(DataFusionError::from)?;
        Ok(PyDataFrame::new(x))
    }

    fn table_exist(&self, name: &str) -> PyResult<bool> {
        Ok(self.ctx.table_exist(name)?)
    }

    fn empty_table(&self) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame::new(self.ctx.read_empty()?))
    }

    fn session_id(&self) -> PyResult<String> {
        Ok(self.ctx.session_id())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (path, schema=None, schema_infer_max_records=1000, file_extension=".json", table_partition_cols=vec![]))]
    fn read_json(
        &mut self,
        path: PathBuf,
        schema: Option<PyArrowType<Schema>>,
        schema_infer_max_records: usize,
        file_extension: &str,
        table_partition_cols: Vec<(String, String)>,
        py: Python,
    ) -> PyResult<PyDataFrame> {
        let path = path
            .to_str()
            .ok_or_else(|| PyValueError::new_err("Unable to convert path to a string"))?;
        let mut options = NdJsonReadOptions::default()
            .table_partition_cols(convert_table_partition_cols(table_partition_cols)?);
        options.schema_infer_max_records = schema_infer_max_records;
        options.file_extension = file_extension;
        let df = if let Some(schema) = schema {
            options.schema = Some(&schema.0);
            let result = self.ctx.read_json(path, options);
            wait_for_future(py, result).map_err(DataFusionError::from)?
        } else {
            let result = self.ctx.read_json(path, options);
            wait_for_future(py, result).map_err(DataFusionError::from)?
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
        table_partition_cols=vec![]))]
    fn read_csv(
        &self,
        path: PathBuf,
        schema: Option<PyArrowType<Schema>>,
        has_header: bool,
        delimiter: &str,
        schema_infer_max_records: usize,
        file_extension: &str,
        table_partition_cols: Vec<(String, String)>,
        py: Python,
    ) -> PyResult<PyDataFrame> {
        let path = path
            .to_str()
            .ok_or_else(|| PyValueError::new_err("Unable to convert path to a string"))?;

        let delimiter = delimiter.as_bytes();
        if delimiter.len() != 1 {
            return Err(PyValueError::new_err(
                "Delimiter must be a single character",
            ));
        };

        let mut options = CsvReadOptions::new()
            .has_header(has_header)
            .delimiter(delimiter[0])
            .schema_infer_max_records(schema_infer_max_records)
            .file_extension(file_extension)
            .table_partition_cols(convert_table_partition_cols(table_partition_cols)?);

        if let Some(py_schema) = schema {
            options.schema = Some(&py_schema.0);
            let result = self.ctx.read_csv(path, options);
            let df = PyDataFrame::new(wait_for_future(py, result).map_err(DataFusionError::from)?);
            Ok(df)
        } else {
            let result = self.ctx.read_csv(path, options);
            let df = PyDataFrame::new(wait_for_future(py, result).map_err(DataFusionError::from)?);
            Ok(df)
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        path,
        table_partition_cols=vec![],
        parquet_pruning=true,
        file_extension=".parquet",
        skip_metadata=true))]
    fn read_parquet(
        &self,
        path: &str,
        table_partition_cols: Vec<(String, String)>,
        parquet_pruning: bool,
        file_extension: &str,
        skip_metadata: bool,
        py: Python,
    ) -> PyResult<PyDataFrame> {
        let mut options = ParquetReadOptions::default()
            .table_partition_cols(convert_table_partition_cols(table_partition_cols)?)
            .parquet_pruning(parquet_pruning)
            .skip_metadata(skip_metadata);
        options.file_extension = file_extension;

        let result = self.ctx.read_parquet(path, options);
        let df = PyDataFrame::new(wait_for_future(py, result).map_err(DataFusionError::from)?);
        Ok(df)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (path, schema=None, table_partition_cols=vec![], file_extension=".avro"))]
    fn read_avro(
        &self,
        path: &str,
        schema: Option<PyArrowType<Schema>>,
        table_partition_cols: Vec<(String, String)>,
        file_extension: &str,
        py: Python,
    ) -> PyResult<PyDataFrame> {
        let mut options = AvroReadOptions::default()
            .table_partition_cols(convert_table_partition_cols(table_partition_cols)?);
        options.file_extension = file_extension;
        let df = if let Some(schema) = schema {
            options.schema = Some(&schema.0);
            let read_future = self.ctx.read_avro(path, options);
            wait_for_future(py, read_future).map_err(DataFusionError::from)?
        } else {
            let read_future = self.ctx.read_avro(path, options);
            wait_for_future(py, read_future).map_err(DataFusionError::from)?
        };
        Ok(PyDataFrame::new(df))
    }

    fn __repr__(&self) -> PyResult<String> {
        let id = self.session_id();
        match id {
            Ok(value) => Ok(format!("SessionContext(session_id={value})")),
            Err(err) => Ok(format!("Error: {:?}", err.to_string())),
        }
    }

    /// Execute a partition of an execution plan and return a stream of record batches
    pub fn execute(
        &self,
        plan: PyExecutionPlan,
        part: usize,
        py: Python,
    ) -> PyResult<PyRecordBatchStream> {
        let ctx = TaskContext::new(
            None,
            "session_id".to_string(),
            SessionConfig::new(),
            HashMap::new(),
            HashMap::new(),
            Arc::new(RuntimeEnv::default()),
        );
        // create a Tokio runtime to run the async code
        let rt = Runtime::new().unwrap();
        let plan = plan.plan.clone();
        let fut: JoinHandle<datafusion_common::Result<SendableRecordBatchStream>> =
            rt.spawn(async move { plan.execute(part, Arc::new(ctx)) });
        let stream = wait_for_future(py, fut).map_err(py_datafusion_err)?;
        Ok(PyRecordBatchStream::new(stream?))
    }
}

impl PySessionContext {
    async fn _table(&self, name: &str) -> datafusion_common::Result<DataFrame> {
        self.ctx.table(name).await
    }
}

fn convert_table_partition_cols(
    table_partition_cols: Vec<(String, String)>,
) -> Result<Vec<(String, DataType)>, DataFusionError> {
    table_partition_cols
        .into_iter()
        .map(|(name, ty)| match ty.as_str() {
            "string" => Ok((name, DataType::Utf8)),
            _ => Err(DataFusionError::Common(format!(
                "Unsupported data type '{ty}' for partition column"
            ))),
        })
        .collect::<Result<Vec<_>, _>>()
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
