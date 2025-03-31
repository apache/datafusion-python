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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::csv::WriterBuilder;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::pyarrow::FromPyArrow;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::TableReference;
use datafusion::prelude::DataFrame;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString, PyTuple};

use crate::errors::{py_datafusion_err, PyDataFusionError, PyDataFusionResult};
use crate::expr::expr::PyExpr;
use crate::expr::window_expr::PyWindowExpr;
use crate::physical_plan::PyExecutionPlan;
use crate::record_batch::{PyRecordBatch, TableData};
use crate::sql::logical::PyLogicalPlan;
use crate::utils::{get_tokio_runtime, wait_for_future};
use crate::Dataset;

/// Represents a DataFrame in DataFusion.
#[pyclass(name = "DataFrame", module = "datafusion", subclass)]
#[derive(Clone)]
pub struct PyDataFrame {
    df: Arc<DataFrame>,
}

#[pymethods]
impl PyDataFrame {
    fn select(&mut self, expr: Vec<PyExpr>) -> PyDataFusion<Self> {
        let expr = expr.into_iter().map(|e| e.into()).collect::<Vec<_>>();
        let df = self.df.select(expr).map_err(py_datafusion_err)?;
        Ok(Self::new(df))
    }

    fn filter(&mut self, predicate: PyExpr) -> PyDataFusionResult<Self> {
        let df = self
            .df
            .filter(predicate.into())
            .map_err(py_datafusion_err)?;
        Ok(Self::new(df))
    }

    fn with_column(&mut self, name: &str, expr: PyExpr) -> PyDataFusionResult<Self> {
        let df = self
            .df
            .with_column(name, expr.into())
            .map_err(py_datafusion_err)?;
        Ok(Self::new(df))
    }

    fn with_columns(&mut self, exprs: Vec<PyExpr>) -> PyDataFusionResult<Self> {
        let mut df = self.df.clone();
        for expr in exprs {
            let expr: Expr = expr.into();
            let name = format!("{}", expr.schema_name());
            df = df
                .with_column(name.as_str(), expr)
                .map_err(py_datafusion_err)?
        }
        Ok(Self::new(df))
    }

    /// Rename one column by applying a new projection. This is a no-op if the column to be
    /// renamed does not exist.
    fn with_column_renamed(&mut self, old_name: &str, new_name: &str) -> PyDataFusionResult<Self> {
        let df = self
            .df
            .with_column_renamed(old_name, new_name)
            .map_err(py_datafusion_err)?;
        Ok(Self::new(df))
    }

    fn aggregate(&mut self, group_by: Vec<PyExpr>, aggs: Vec<PyExpr>) -> PyDataFusionResult<Self> {
        let group_by = group_by.into_iter().map(|e| e.into()).collect();
        let aggs = aggs.into_iter().map(|e| e.into()).collect();
        let df = self
            .df
            .aggregate(group_by, aggs)
            .map_err(py_datafusion_err)?;
        Ok(Self::new(df))
    }

    #[pyo3(signature = (*exprs))]
    fn sort(&mut self, exprs: Vec<PyWindowExpr>) -> PyDataFusionResult<Self> {
        let exprs = to_sort_expressions(exprs);
        let df = self.df.sort(exprs).map_err(py_datafusion_err)?;
        Ok(Self::new(df))
    }

    #[pyo3(signature = (count, offset=0))]
    fn limit(&mut self, count: usize, offset: usize) -> PyDataFusionResult<Self> {
        let df = self
            .df
            .limit(offset, Some(count))
            .map_err(py_datafusion_err)?;
        Ok(Self::new(df))
    }

    /// Executes the plan, returning a list of `RecordBatch`es.
    /// Unless some order is specified in the plan, there is no
    /// guarantee of the order of the result.
    fn collect(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let batches =
            wait_for_future(py, self.df.clone().collect()).map_err(PyDataFusionError::from)?;
        // cannot use PyResult<Vec<RecordBatch>> return type due to
        // https://github.com/PyO3/pyo3/issues/1813
        batches.into_iter().map(|rb| rb.to_pyarrow(py)).collect()
    }

    /// Cache DataFrame.
    fn cache(&mut self, py: Python) -> PyDataFusionResult<Self> {
        let df = wait_for_future(py, self.df.clone().cache())?;
        Ok(Self::new(df))
    }

    /// Executes this DataFrame and collects all results into a vector of vector of RecordBatch
    /// maintaining the input partitioning.
    fn collect_partitioned(&self, py: Python) -> PyResult<Vec<Vec<PyObject>>> {
        let batches = wait_for_future(py, self.df.clone().collect_partitioned())
            .map_err(PyDataFusionError::from)?;

        batches
            .into_iter()
            .map(|rbs| rbs.into_iter().map(|rb| rb.to_pyarrow(py)).collect())
            .collect()
    }

    /// Print the result, 20 lines by default
    #[pyo3(signature = (num=20))]
    fn show(&self, py: Python, num: usize) -> PyDataFusionResult<()> {
        let df = self
            .df
            .clone()
            .limit(0, Some(num))
            .map_err(py_datafusion_err)?;
        print_dataframe(py, df)
    }

    /// Filter out duplicate rows
    fn distinct(&mut self) -> PyDataFusionResult<Self> {
        let df = self.df.clone().distinct().map_err(py_datafusion_err)?;
        Ok(Self::new(df))
    }

    fn join(
        &mut self,
        right: PyDataFrame,
        how: &str,
        left_on: Vec<PyBackedStr>,
        right_on: Vec<PyBackedStr>,
    ) -> PyDataFusionResult<Self> {
        let join_type = match how {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "full" => JoinType::Full,
            "semi" => JoinType::LeftSemi,
            "anti" => JoinType::LeftAnti,
            how => {
                return Err(PyDataFusionError::Common(format!(
                    "The join type {how} does not exist or is not implemented"
                )));
            }
        };

        let left_keys = left_on.iter().map(|s| s.as_ref()).collect::<Vec<&str>>();
        let right_keys = right_on.iter().map(|s| s.as_ref()).collect::<Vec<&str>>();

        let df = self
            .df
            .join(right.df.clone(), join_type, &left_keys, &right_keys, None)?;
        Ok(Self::new(df))
    }

    fn join_on(
        &mut self,
        right: PyDataFrame,
        on_exprs: Vec<PyExpr>,
        how: &str,
    ) -> PyDataFusionResult<Self> {
        let join_type = match how {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "full" => JoinType::Full,
            "semi" => JoinType::LeftSemi,
            "anti" => JoinType::LeftAnti,
            how => {
                return Err(PyDataFusionError::Common(format!(
                    "The join type {how} does not exist or is not implemented"
                )));
            }
        };
        let exprs: Vec<Expr> = on_exprs.into_iter().map(|e| e.into()).collect();

        let df = self
            .df
            .join_on(right.df.clone(), join_type, exprs)
            .map_err(py_datafusion_err)?;
        Ok(Self::new(df))
    }

    /// Print the query plan
    #[pyo3(signature = (verbose=false, analyze=false))]
    fn explain(&self, py: Python, verbose: bool, analyze: bool) -> PyDataFusionResult<()> {
        let df = self
            .df
            .explain(verbose, analyze)
            .map_err(py_datafusion_err)?;
        print_dataframe(py, df)
    }

    /// Get the logical plan for this `DataFrame`
    fn logical_plan(&self) -> PyResult<PyLogicalPlan> {
        Ok(self.df.logical_plan().clone().into())
    }

    /// Get the optimized logical plan for this `DataFrame`
    fn optimized_logical_plan(&self) -> PyDataFusionResult<PyLogicalPlan> {
        Ok(self.df.clone().into_optimized_plan()?.into())
    }

    /// Get the execution plan for this `DataFrame`
    fn execution_plan(&self, py: Python) -> PyDataFusionResult<PyExecutionPlan> {
        let plan = wait_for_future(py, self.df.clone().create_physical_plan())?;
        Ok(plan.into())
    }

    /// Repartition a `DataFrame` based on a logical partitioning scheme.
    fn repartition(&self, num: usize) -> PyDataFusionResult<Self> {
        let new_df = self
            .df
            .repartition(Partitioning::RoundRobinBatch(num))
            .map_err(py_datafusion_err)?;
        Ok(Self::new(new_df))
    }

    /// Repartition a `DataFrame` based on a logical partitioning scheme.
    #[pyo3(signature = (*args, num))]
    fn repartition_by_hash(&self, args: Vec<PyExpr>, num: usize) -> PyDataFusionResult<Self> {
        let expr = args.into_iter().map(|py_expr| py_expr.into()).collect();
        let new_df = self
            .df
            .repartition(Partitioning::Hash(expr, num))
            .map_err(py_datafusion_err)?;
        Ok(Self::new(new_df))
    }

    /// Calculate the union of two `DataFrame`s, preserving duplicate rows.The
    /// two `DataFrame`s must have exactly the same schema
    #[pyo3(signature = (py_df, distinct=false))]
    fn union(&self, py_df: PyDataFrame, distinct: bool) -> PyDataFusionResult<Self> {
        let new_df = if distinct {
            self.df
                .union_distinct(py_df.df.clone())
                .map_err(py_datafusion_err)?
        } else {
            self.df
                .clone()
                .union(py_df.df.clone())
                .map_err(py_datafusion_err)?
        };

        Ok(Self::new(new_df))
    }

    /// Calculate the distinct union of two `DataFrame`s.  The
    /// two `DataFrame`s must have exactly the same schema
    fn union_distinct(&self, py_df: PyDataFrame) -> PyDataFusionResult<Self> {
        let new_df = self
            .df
            .union_distinct(py_df.df.clone())
            .map_err(py_datafusion_err)?;
        Ok(Self::new(new_df))
    }

    #[pyo3(signature = (column, preserve_nulls=true))]
    fn unnest_column(&self, column: &str, preserve_nulls: bool) -> PyDataFusionResult<Self> {
        // TODO: expose RecursionUnnestOptions
        // REF: https://github.com/apache/datafusion/pull/11577
        let unnest_options = UnnestOptions::default().with_preserve_nulls(preserve_nulls);
        let df = self
            .df
            .unnest_columns_with_options(&[column], unnest_options)
            .map_err(py_datafusion_err)?;
        Ok(Self::new(df))
    }

    #[pyo3(signature = (columns, preserve_nulls=true))]
    fn unnest_columns(
        &self,
        columns: Vec<String>,
        preserve_nulls: bool,
    ) -> PyDataFusionResult<Self> {
        // TODO: expose RecursionUnnestOptions
        // REF: https://github.com/apache/datafusion/pull/11577
        let unnest_options = UnnestOptions::default().with_preserve_nulls(preserve_nulls);
        let cols = columns.iter().map(|s| s.as_ref()).collect::<Vec<&str>>();
        let df = self
            .df
            .unnest_columns_with_options(&cols, unnest_options)
            .map_err(py_datafusion_err)?;
        Ok(Self::new(df))
    }

    /// Calculate the intersection of two `DataFrame`s.  The two `DataFrame`s must have exactly the same schema
    fn intersect(&self, py_df: PyDataFrame) -> PyDataFusionResult<Self> {
        let new_df = self
            .df
            .intersect(py_df.df.clone())
            .map_err(py_datafusion_err)?;
        Ok(Self::new(new_df))
    }

    /// Calculate the exception of two `DataFrame`s.  The two `DataFrame`s must have exactly the same schema
    fn except_all(&self, py_df: PyDataFrame) -> PyDataFusionResult<Self> {
        let new_df = self
            .df
            .clone()
            .except(py_df.df.clone())
            .map_err(py_datafusion_err)?;
        Ok(Self::new(new_df))
    }

    /// Write a `DataFrame` to a CSV file.
    fn write_csv(&self, path: &str, with_header: bool, py: Python) -> PyDataFusionResult<()> {
        let csv_options = CsvOptions {
            has_header: Some(with_header),
            ..Default::default()
        };
        wait_for_future(
            py,
            self.df
                .clone()
                .write_csv(path, DataFrameWriteOptions::new(), Some(csv_options)),
        )?;
        Ok(())
    }

    /// Write a `DataFrame` to a Parquet file.
    #[pyo3(signature = (
        path,
        compression="zstd",
        compression_level=None
        ))]
    fn write_parquet(
        &self,
        path: &str,
        compression: &str,
        compression_level: Option<u32>,
        py: Python,
    ) -> PyDataFusionResult<()> {
        fn verify_compression_level(cl: Option<u32>) -> Result<u32, PyErr> {
            cl.ok_or(PyValueError::new_err("compression_level is not defined"))
        }

        let _validated = match compression.to_lowercase().as_str() {
            "snappy" => Compression::SNAPPY,
            "gzip" => Compression::GZIP(
                GzipLevel::try_new(compression_level.unwrap_or(6))
                    .map_err(|e| PyValueError::new_err(format!("{e}")))?,
            ),
            "brotli" => Compression::BROTLI(
                BrotliLevel::try_new(verify_compression_level(compression_level)?)
                    .map_err(|e| PyValueError::new_err(format!("{e}")))?,
            ),
            "zstd" => Compression::ZSTD(
                ZstdLevel::try_new(verify_compression_level(compression_level)? as i32)
                    .map_err(|e| PyValueError::new_err(format!("{e}")))?,
            ),
            "lzo" => Compression::LZO,
            "lz4" => Compression::LZ4,
            "lz4_raw" => Compression::LZ4_RAW,
            "uncompressed" => Compression::UNCOMPRESSED,
            _ => {
                return Err(PyDataFusionError::Common(format!(
                    "Unrecognized compression type {compression}"
                )));
            }
        };

        let mut compression_string = compression.to_string();
        if let Some(level) = compression_level {
            compression_string.push_str(&format!("({level})"));
        }

        let mut options = TableParquetOptions::default();
        options.global.compression = Some(compression_string);

        wait_for_future(
            py,
            self.df.clone().write_parquet(
                path,
                DataFrameWriteOptions::new(),
                Option::from(options),
            ),
        )?;
        Ok(())
    }

    /// Executes a query and writes the results to a partitioned JSON file.
    fn write_json(&self, path: &str, py: Python) -> PyDataFusionResult<()> {
        wait_for_future(
            py,
            self.df
                .clone()
                .write_json(path, DataFrameWriteOptions::new(), None),
        )?;
        Ok(())
    }

    /// Convert to Arrow Table
    /// Collect the batches and pass to Arrow Table
    fn to_arrow_table(&self, py: Python<'_>) -> PyResult<PyObject> {
        let batches = self.collect(py)?.into_pyobject(py)?;
        let schema = self.schema().into_pyobject(py)?;

        // Instantiate pyarrow Table object and use its from_batches method
        let table_class = py.import("pyarrow")?.getattr("Table")?;
        let args = PyTuple::new(py, &[batches, schema])?;
        let table: PyObject = table_class.call_method1("from_batches", args)?.into();
        Ok(table)
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &'py mut self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyDataFusionResult<Bound<'py, PyCapsule>> {
        let mut batches = wait_for_future(py, self.df.clone().collect())?;
        let mut schema: Schema = self.df.schema().to_owned().into();

        if let Some(schema_capsule) = requested_schema {
            validate_pycapsule(&schema_capsule, "arrow_schema")?;

            let schema_ptr = unsafe { schema_capsule.reference::<FFI_ArrowSchema>() };
            let desired_schema = Schema::try_from(schema_ptr)?;

            schema = project_schema(schema, desired_schema)?;

            batches = batches
                .into_iter()
                .map(|record_batch| record_batch_into_schema(record_batch, &schema))
                .collect::<Result<Vec<RecordBatch>, ArrowError>>()?;
        }

        let batches_wrapped = batches.into_iter().map(Ok);

        let reader = RecordBatchIterator::new(batches_wrapped, Arc::new(schema));
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(reader);

        let ffi_stream = FFI_ArrowArrayStream::new(reader);
        let stream_capsule_name = CString::new("arrow_array_stream").unwrap();
        PyCapsule::new(py, ffi_stream, Some(stream_capsule_name)).map_err(PyDataFusionError::from)
    }

    fn execute_stream(&self, py: Python) -> PyDataFusionResult<PyRecordBatchStream> {
        // create a Tokio runtime to run the async code
        let rt = &get_tokio_runtime().0;
        let df = self.df.clone();
        let fut: JoinHandle<datafusion::common::Result<SendableRecordBatchStream>> =
            rt.spawn(async move { df.execute_stream().await });
        let stream = wait_for_future(py, fut).map_err(py_datafusion_err)?;
        Ok(PyRecordBatchStream::new(stream?))
    }

    fn execute_stream_partitioned(&self, py: Python) -> PyResult<Vec<PyRecordBatchStream>> {
        // create a Tokio runtime to run the async code
        let rt = &get_tokio_runtime().0;
        let df = self.df.clone();
        let fut: JoinHandle<datafusion::common::Result<Vec<SendableRecordBatchStream>>> =
            rt.spawn(async move { df.execute_stream_partitioned().await });
        let stream = wait_for_future(py, fut).map_err(py_datafusion_err)?;

        match stream {
            Ok(batches) => Ok(batches.into_iter().map(PyRecordBatchStream::new).collect()),
            _ => Err(PyValueError::new_err(
                "Unable to execute stream partitioned",
            )),
        }
    }

    /// Convert to pandas dataframe with pyarrow
    /// Collect the batches, pass to Arrow Table & then convert to Pandas DataFrame
    fn to_pandas(&self, py: Python<'_>) -> PyResult<PyObject> {
        let table = self.to_arrow_table(py)?;

        // See also: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.to_pandas
        let result = table.call_method0(py, "to_pandas")?;
        Ok(result)
    }

    /// Convert to Python list using pyarrow
    /// Each list item represents one row encoded as dictionary
    fn to_pylist(&self, py: Python<'_>) -> PyResult<PyObject> {
        let table = self.to_arrow_table(py)?;

        // See also: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.to_pylist
        let result = table.call_method0(py, "to_pylist")?;
        Ok(result)
    }

    /// Convert to Python dictionary using pyarrow
    /// Each dictionary key is a column and the dictionary value represents the column values
    fn to_pydict(&self, py: Python) -> PyResult<PyObject> {
        let table = self.to_arrow_table(py)?;

        // See also: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.to_pydict
        let result = table.call_method0(py, "to_pydict")?;
        Ok(result)
    }

    /// Convert to polars dataframe with pyarrow
    /// Collect the batches, pass to Arrow Table & then convert to polars DataFrame
    fn to_polars(&self, py: Python<'_>) -> PyResult<PyObject> {
        let table = self.to_arrow_table(py)?;
        let dataframe = py.import("polars")?.getattr("DataFrame")?;
        let args = PyTuple::new(py, &[table])?;
        let result: PyObject = dataframe.call1(args)?.into();
        Ok(result)
    }

    // Executes this DataFrame to get the total number of rows.
    fn count(&self, py: Python) -> PyDataFusionResult<usize> {
        Ok(wait_for_future(py, self.df.clone().count())?)
    }

    #[pyo3(signature = (max_width=None, max_rows=None, show_nulls=None))]
    pub fn to_string(
        &self,
        max_width: Option<usize>,
        max_rows: Option<usize>,
        show_nulls: Option<bool>,
        py: Python,
    ) -> PyDataFusionResult<String> {
        let batches = wait_for_future(py, self.df.clone().collect())?;

        let mut table = TableData::new(&batches)?;

        // Use the display configuration provided or default values
        let max_width = max_width.unwrap_or(80);
        let max_rows = max_rows;
        let show_nulls = show_nulls.unwrap_or(false);

        table.set_display_options(max_width, max_rows, show_nulls);

        Ok(table.to_string())
    }

    pub fn __repr__(&self, py: Python) -> PyDataFusionResult<String> {
        // Use default display configuration
        self.to_string(None, None, None, py)
    }
}

impl PyDataFrame {
    pub fn new(df: DataFrame) -> Self {
        Self { df: Arc::new(df) }
    }

    pub fn dataframe(&self) -> Arc<DataFrame> {
        self.df.clone()
    }
}
