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
use uuid::Uuid;

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;

use parking_lot::RwLock;

use crate::catalog::{PyCatalog, PyTable};
use crate::dataframe::PyDataFrame;
use crate::dataset::Dataset;
use crate::errors::DataFusionError;
use crate::store::StorageContexts;
use crate::udaf::PyAggregateUDF;
use crate::udf::PyScalarUDF;
use crate::utils::wait_for_future;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::config::ConfigOptions;
use datafusion::datasource::datasource::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::prelude::{AvroReadOptions, CsvReadOptions, NdJsonReadOptions, ParquetReadOptions};

/// `PySessionContext` is able to plan and execute DataFusion plans.
/// It has a powerful optimizer, a physical planner for local execution, and a
/// multi-threaded execution engine to perform the execution.
#[pyclass(name = "SessionContext", module = "datafusion", subclass, unsendable)]
pub(crate) struct PySessionContext {
    ctx: SessionContext,
}

#[pymethods]
impl PySessionContext {
    #[allow(clippy::too_many_arguments)]
    #[args(
        default_catalog = "\"datafusion\"",
        default_schema = "\"public\"",
        create_default_catalog_and_schema = "true",
        information_schema = "false",
        repartition_joins = "true",
        repartition_aggregations = "true",
        repartition_windows = "true",
        parquet_pruning = "true",
        target_partitions = "None",
        config_options = "None"
    )]
    #[new]
    fn new(
        default_catalog: &str,
        default_schema: &str,
        create_default_catalog_and_schema: bool,
        information_schema: bool,
        repartition_joins: bool,
        repartition_aggregations: bool,
        repartition_windows: bool,
        parquet_pruning: bool,
        target_partitions: Option<usize>,
        config_options: Option<HashMap<String, String>>,
    ) -> Self {
        let mut options = ConfigOptions::from_env();
        if let Some(hash_map) = config_options {
            for (k, v) in &hash_map {
                if let Ok(v) = v.parse::<bool>() {
                    options.set_bool(k, v);
                } else if let Ok(v) = v.parse::<u64>() {
                    options.set_u64(k, v);
                } else {
                    options.set_string(k, v);
                }
            }
        }
        let config_options = Arc::new(RwLock::new(options));

        let mut cfg = SessionConfig::new()
            .create_default_catalog_and_schema(create_default_catalog_and_schema)
            .with_default_catalog_and_schema(default_catalog, default_schema)
            .with_information_schema(information_schema)
            .with_repartition_joins(repartition_joins)
            .with_repartition_aggregations(repartition_aggregations)
            .with_repartition_windows(repartition_windows)
            .with_parquet_pruning(parquet_pruning);

        // TODO we should add a `with_config_options` to `SessionConfig`
        cfg.config_options = config_options;

        let cfg_full = match target_partitions {
            None => cfg,
            Some(x) => cfg.with_target_partitions(x),
        };

        PySessionContext {
            ctx: SessionContext::with_config(cfg_full),
        }
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

        self.ctx
            .runtime_env()
            .register_object_store(scheme, derived_host, store);
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
    ) -> PyResult<PyDataFrame> {
        let schema = partitions.0[0][0].schema();
        let table = MemTable::try_new(schema, partitions.0).map_err(DataFusionError::from)?;

        // generate a random (unique) name for this table
        // table name cannot start with numeric digit
        let name = "c".to_owned()
            + Uuid::new_v4()
                .to_simple()
                .encode_lower(&mut Uuid::encode_buffer());

        self.ctx
            .register_table(&*name, Arc::new(table))
            .map_err(DataFusionError::from)?;
        let table = self.ctx.table(&*name).map_err(DataFusionError::from)?;

        let df = PyDataFrame::new(table);
        Ok(df)
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
    #[args(
        table_partition_cols = "vec![]",
        parquet_pruning = "true",
        file_extension = "\".parquet\""
    )]
    fn register_parquet(
        &mut self,
        name: &str,
        path: &str,
        table_partition_cols: Vec<String>,
        parquet_pruning: bool,
        file_extension: &str,
        py: Python,
    ) -> PyResult<()> {
        let mut options = ParquetReadOptions::default()
            .table_partition_cols(table_partition_cols)
            .parquet_pruning(parquet_pruning);
        options.file_extension = file_extension;
        let result = self.ctx.register_parquet(name, path, options);
        wait_for_future(py, result).map_err(DataFusionError::from)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[args(
        schema = "None",
        has_header = "true",
        delimiter = "\",\"",
        schema_infer_max_records = "1000",
        file_extension = "\".csv\""
    )]
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

    #[args(name = "\"datafusion\"")]
    fn catalog(&self, name: &str) -> PyResult<PyCatalog> {
        match self.ctx.catalog(name) {
            Some(catalog) => Ok(PyCatalog::new(catalog)),
            None => Err(PyKeyError::new_err(format!(
                "Catalog with name {} doesn't exist.",
                &name
            ))),
        }
    }

    fn tables(&self) -> HashSet<String> {
        #[allow(deprecated)]
        self.ctx.tables().unwrap()
    }

    fn table(&self, name: &str) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame::new(self.ctx.table(name)?))
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
    #[args(
        schema = "None",
        schema_infer_max_records = "1000",
        file_extension = "\".json\"",
        table_partition_cols = "vec![]"
    )]
    fn read_json(
        &mut self,
        path: PathBuf,
        schema: Option<PyArrowType<Schema>>,
        schema_infer_max_records: usize,
        file_extension: &str,
        table_partition_cols: Vec<String>,
        py: Python,
    ) -> PyResult<PyDataFrame> {
        let path = path
            .to_str()
            .ok_or_else(|| PyValueError::new_err("Unable to convert path to a string"))?;

        let mut options = NdJsonReadOptions::default().table_partition_cols(table_partition_cols);
        options.schema = schema.map(|s| Arc::new(s.0));
        options.schema_infer_max_records = schema_infer_max_records;
        options.file_extension = file_extension;

        let result = self.ctx.read_json(path, options);
        let df = wait_for_future(py, result).map_err(DataFusionError::from)?;
        Ok(PyDataFrame::new(df))
    }

    #[allow(clippy::too_many_arguments)]
    #[args(
        schema = "None",
        has_header = "true",
        delimiter = "\",\"",
        schema_infer_max_records = "1000",
        file_extension = "\".csv\"",
        table_partition_cols = "vec![]"
    )]
    fn read_csv(
        &self,
        path: PathBuf,
        schema: Option<PyArrowType<Schema>>,
        has_header: bool,
        delimiter: &str,
        schema_infer_max_records: usize,
        file_extension: &str,
        table_partition_cols: Vec<String>,
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
            .table_partition_cols(table_partition_cols);

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
    #[args(
        parquet_pruning = "true",
        file_extension = "\".parquet\"",
        table_partition_cols = "vec![]",
        skip_metadata = "true"
    )]
    fn read_parquet(
        &self,
        path: &str,
        table_partition_cols: Vec<String>,
        parquet_pruning: bool,
        file_extension: &str,
        skip_metadata: bool,
        py: Python,
    ) -> PyResult<PyDataFrame> {
        let mut options = ParquetReadOptions::default()
            .table_partition_cols(table_partition_cols)
            .parquet_pruning(parquet_pruning)
            .skip_metadata(skip_metadata);
        options.file_extension = file_extension;

        let result = self.ctx.read_parquet(path, options);
        let df = PyDataFrame::new(wait_for_future(py, result).map_err(DataFusionError::from)?);
        Ok(df)
    }

    #[allow(clippy::too_many_arguments)]
    #[args(
        schema = "None",
        file_extension = "\".avro\"",
        table_partition_cols = "vec![]"
    )]
    fn read_avro(
        &self,
        path: &str,
        schema: Option<PyArrowType<Schema>>,
        table_partition_cols: Vec<String>,
        file_extension: &str,
        py: Python,
    ) -> PyResult<PyDataFrame> {
        let mut options = AvroReadOptions::default().table_partition_cols(table_partition_cols);
        options.file_extension = file_extension;
        options.schema = schema.map(|s| Arc::new(s.0));

        let result = self.ctx.read_avro(path, options);
        let df = PyDataFrame::new(wait_for_future(py, result).map_err(DataFusionError::from)?);
        Ok(df)
    }
}
