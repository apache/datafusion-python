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

//! A table provider over a directory of Parquet files.
//!
//! One output partition per file, which is the whole reason this provider
//! exists rather than `SessionContext.register_parquet`: it fixes the mapping
//! from partition index to file, so a distributed engine can hand partition
//! `i` to a worker and know exactly which bytes that worker will read.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result, plan_err};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::parquet::arrow::parquet_to_arrow_schema;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_python_util::ffi_logical_codec_from_pycapsule;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::exec::{FileSlice, PartitionedParquetExec};

/// Scans `*.parquet` under `directory`, one partition per file.
#[derive(Debug)]
pub(crate) struct PartitionedParquetTable {
    /// Kept so the logical codec can write it down. The file list is derived
    /// from the directory, so the path is the whole encoding -- see
    /// [`crate::codec::DfxStorageLogicalCodec`]. The schema is not derived on
    /// the decode path; see [`Self::try_new_with_schema`].
    pub(crate) directory: String,
    files: Vec<FileSlice>,
    schema: SchemaRef,
}

impl PartitionedParquetTable {
    /// Read the directory listing and the first file's schema, once.
    ///
    /// For the registration path, where nobody has told us the schema yet.
    pub(crate) fn try_new(directory: &Path) -> Result<Self> {
        Self::open(directory, None)
    }

    /// Open with the schema the plan was built against, rather than re-reading
    /// it from a file.
    ///
    /// This is the decode path, and taking the caller's schema is not an
    /// optimisation. A serialized `CustomScan` carries the table schema and
    /// its projection as *column names*; the decoder turns those names into
    /// indices against the encoded schema and then applies them to whatever
    /// this provider reports. Re-reading a footer here would let the two
    /// drift, and the indices are applied without a bounds check -- so a
    /// column added to the directory since the plan was written silently
    /// selects the wrong one, and a column removed panics inside DataFusion.
    /// Neither is reachable if the schema in the plan is the schema used.
    pub(crate) fn try_new_with_schema(directory: &Path, schema: SchemaRef) -> Result<Self> {
        Self::open(directory, Some(schema))
    }

    /// Sorted by path so that partition `i` means the same file in every
    /// process that opens the same directory. Directory iteration order is
    /// not specified, and a worker that disagreed with the driver about which
    /// file is partition 3 would silently produce wrong answers.
    fn open(directory: &Path, schema: Option<SchemaRef>) -> Result<Self> {
        let mut paths: Vec<_> = fs::read_dir(directory)
            .map_err(|err| DataFusionError::External(Box::new(err)))?
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|err| DataFusionError::External(Box::new(err)))?
            .into_iter()
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
            .collect();
        paths.sort();

        if paths.is_empty() {
            return plan_err!("no .parquet files under {}", directory.display());
        }

        let mut files = Vec::with_capacity(paths.len());
        for path in &paths {
            let metadata =
                fs::metadata(path).map_err(|err| DataFusionError::External(Box::new(err)))?;
            let path = path
                .to_str()
                .ok_or_else(|| DataFusionError::Plan(format!("non-UTF-8 path {path:?}")))?;
            files.push(FileSlice {
                path: path.to_string(),
                size: metadata.len(),
            });
        }

        let schema = match schema {
            Some(schema) => schema,
            None => Arc::new(Self::read_schema(&paths[0])?),
        };
        Ok(Self {
            directory: directory.to_string_lossy().into_owned(),
            files,
            schema,
        })
    }

    fn read_schema(path: &Path) -> Result<Schema> {
        let file = fs::File::open(path).map_err(|err| DataFusionError::External(Box::new(err)))?;
        let reader = SerializedFileReader::new(file)
            .map_err(|err| DataFusionError::ParquetError(Box::new(err)))?;
        let metadata = reader.metadata().file_metadata();
        parquet_to_arrow_schema(metadata.schema_descr(), metadata.key_value_metadata())
            .map_err(|err| DataFusionError::ParquetError(Box::new(err)))
    }
}

#[async_trait]
impl TableProvider for PartitionedParquetTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Every filter is re-applied above the scan. Claiming `Exact` would
        // tell the optimizer to drop the `FilterExec`, and this node does not
        // pass predicates down to the Parquet reader.
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(PartitionedParquetExec::new(
            self.files.clone(),
            Arc::clone(&self.schema),
            projection.cloned(),
            limit,
        )?))
    }
}

/// Python handle for [`PartitionedParquetTable`].
#[pyclass(name = "PartitionedParquetTable", module = "dfx_storage")]
pub(crate) struct PyPartitionedParquetTable {
    directory: String,
}

#[pymethods]
impl PyPartitionedParquetTable {
    /// Open every `*.parquet` file under `directory` as one table.
    #[new]
    fn new(directory: String) -> Self {
        Self { directory }
    }

    /// Number of files, and so the number of output partitions.
    fn partition_count(&self) -> PyResult<usize> {
        Ok(self.build()?.files.len())
    }

    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let provider = Arc::new(self.build()?);
        // The codec comes off the session this provider is being installed
        // on, never from a `SessionContext` built here: one built inline is
        // already dropped by the time the capsule is used.
        let codec = ffi_logical_codec_from_pycapsule(session, None)?;
        let ffi = FFI_TableProvider::new_with_ffi_codec(provider, false, None, codec);
        PyCapsule::new_with_value(py, ffi, cr"datafusion_table_provider")
    }
}

impl PyPartitionedParquetTable {
    fn build(&self) -> PyResult<PartitionedParquetTable> {
        PartitionedParquetTable::try_new(Path::new(&self.directory))
            .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))
    }
}
