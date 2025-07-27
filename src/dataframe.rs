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
use std::ffi::CString;
use std::sync::Arc;

use arrow::array::{new_null_array, RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow::compute::can_cast_types;
use arrow::error::ArrowError;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::pyarrow::FromPyArrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::pyarrow::{PyArrowType, ToPyArrow};
use datafusion::arrow::util::pretty;
use datafusion::common::UnnestOptions;
use datafusion::config::{CsvOptions, ParquetColumnOptions, ParquetOptions, TableParquetOptions};
use datafusion::dataframe::{DataFrame, DataFrameWriteOptions};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use datafusion::prelude::*;
use datafusion_ffi::table_provider::FFI_TableProvider;
use futures::{StreamExt, TryStreamExt};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyCapsule, PyList, PyTuple, PyTupleMethods};
use tokio::task::JoinHandle;

use crate::catalog::PyTable;
use crate::errors::{py_datafusion_err, to_datafusion_err, PyDataFusionError};
use crate::expr::sort_expr::to_sort_expressions;
use crate::physical_plan::PyExecutionPlan;
use crate::record_batch::PyRecordBatchStream;
use crate::sql::logical::PyLogicalPlan;
use crate::utils::{
    get_tokio_runtime, is_ipython_env, py_obj_to_scalar_value, validate_pycapsule, wait_for_future,
};
use crate::{
    errors::PyDataFusionResult,
    expr::{sort_expr::PySortExpr, PyExpr},
};

// https://github.com/apache/datafusion-python/pull/1016#discussion_r1983239116
// - we have not decided on the table_provider approach yet
// this is an interim implementation
#[pyclass(name = "TableProvider", module = "datafusion")]
pub struct PyTableProvider {
    provider: Arc<dyn TableProvider + Send>,
}

impl PyTableProvider {
    pub fn new(provider: Arc<dyn TableProvider>) -> Self {
        Self { provider }
    }

    pub fn as_table(&self) -> PyTable {
        let table_provider: Arc<dyn TableProvider> = self.provider.clone();
        PyTable::new(table_provider)
    }
}

#[pymethods]
impl PyTableProvider {
    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = CString::new("datafusion_table_provider").unwrap();

        let runtime = get_tokio_runtime().0.handle().clone();
        let provider = FFI_TableProvider::new(Arc::clone(&self.provider), false, Some(runtime));

        PyCapsule::new(py, provider, Some(name.clone()))
    }
}

/// Configuration for DataFrame display formatting
#[derive(Debug, Clone)]
pub struct FormatterConfig {
    /// Maximum memory in bytes to use for display (default: 2MB)
    pub max_bytes: usize,
    /// Minimum number of rows to display (default: 20)
    pub min_rows: usize,
    /// Number of rows to include in __repr__ output (default: 10)
    pub repr_rows: usize,
}

impl Default for FormatterConfig {
    fn default() -> Self {
        Self {
            max_bytes: 2 * 1024 * 1024, // 2MB
            min_rows: 20,
            repr_rows: 10,
        }
    }
}

impl FormatterConfig {
    /// Validates that all configuration values are positive integers.
    ///
    /// # Returns
    ///
    /// `Ok(())` if all values are valid, or an `Err` with a descriptive error message.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_bytes == 0 {
            return Err("max_bytes must be a positive integer".to_string());
        }

        if self.min_rows == 0 {
            return Err("min_rows must be a positive integer".to_string());
        }

        if self.repr_rows == 0 {
            return Err("repr_rows must be a positive integer".to_string());
        }

        Ok(())
    }
}

/// Holds the Python formatter and its configuration
struct PythonFormatter<'py> {
    /// The Python formatter object
    formatter: Bound<'py, PyAny>,
    /// The formatter configuration
    config: FormatterConfig,
}

/// Get the Python formatter and its configuration
fn get_python_formatter_with_config(py: Python) -> PyResult<PythonFormatter> {
    let formatter = import_python_formatter(py)?;
    let config = build_formatter_config_from_python(&formatter)?;
    Ok(PythonFormatter { formatter, config })
}

/// Get the Python formatter from the datafusion.dataframe_formatter module
fn import_python_formatter(py: Python) -> PyResult<Bound<'_, PyAny>> {
    let formatter_module = py.import("datafusion.dataframe_formatter")?;
    let get_formatter = formatter_module.getattr("get_formatter")?;
    get_formatter.call0()
}

// Helper function to extract attributes with fallback to default
fn get_attr<'a, T>(py_object: &'a Bound<'a, PyAny>, attr_name: &str, default_value: T) -> T
where
    T: for<'py> pyo3::FromPyObject<'py> + Clone,
{
    py_object
        .getattr(attr_name)
        .and_then(|v| v.extract::<T>())
        .unwrap_or_else(|_| default_value.clone())
}

/// Helper function to create a FormatterConfig from a Python formatter object
fn build_formatter_config_from_python(formatter: &Bound<'_, PyAny>) -> PyResult<FormatterConfig> {
    let default_config = FormatterConfig::default();
    let max_bytes = get_attr(formatter, "max_memory_bytes", default_config.max_bytes);
    let min_rows = get_attr(formatter, "min_rows_display", default_config.min_rows);
    let repr_rows = get_attr(formatter, "repr_rows", default_config.repr_rows);

    let config = FormatterConfig {
        max_bytes,
        min_rows,
        repr_rows,
    };

    // Return the validated config, converting String error to PyErr
    config.validate().map_err(PyValueError::new_err)?;
    Ok(config)
}

/// Python mapping of `ParquetOptions` (includes just the writer-related options).
#[pyclass(name = "ParquetWriterOptions", module = "datafusion", subclass)]
#[derive(Clone, Default)]
pub struct PyParquetWriterOptions {
    options: ParquetOptions,
}

#[pymethods]
impl PyParquetWriterOptions {
    #[new]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        data_pagesize_limit: usize,
        write_batch_size: usize,
        writer_version: String,
        skip_arrow_metadata: bool,
        compression: Option<String>,
        dictionary_enabled: Option<bool>,
        dictionary_page_size_limit: usize,
        statistics_enabled: Option<String>,
        max_row_group_size: usize,
        created_by: String,
        column_index_truncate_length: Option<usize>,
        statistics_truncate_length: Option<usize>,
        data_page_row_count_limit: usize,
        encoding: Option<String>,
        bloom_filter_on_write: bool,
        bloom_filter_fpp: Option<f64>,
        bloom_filter_ndv: Option<u64>,
        allow_single_file_parallelism: bool,
        maximum_parallel_row_group_writers: usize,
        maximum_buffered_record_batches_per_stream: usize,
    ) -> Self {
        Self {
            options: ParquetOptions {
                data_pagesize_limit,
                write_batch_size,
                writer_version,
                skip_arrow_metadata,
                compression,
                dictionary_enabled,
                dictionary_page_size_limit,
                statistics_enabled,
                max_row_group_size,
                created_by,
                column_index_truncate_length,
                statistics_truncate_length,
                data_page_row_count_limit,
                encoding,
                bloom_filter_on_write,
                bloom_filter_fpp,
                bloom_filter_ndv,
                allow_single_file_parallelism,
                maximum_parallel_row_group_writers,
                maximum_buffered_record_batches_per_stream,
                ..Default::default()
            },
        }
    }
}

/// Python mapping of `ParquetColumnOptions`.
#[pyclass(name = "ParquetColumnOptions", module = "datafusion", subclass)]
#[derive(Clone, Default)]
pub struct PyParquetColumnOptions {
    options: ParquetColumnOptions,
}

#[pymethods]
impl PyParquetColumnOptions {
    #[new]
    pub fn new(
        bloom_filter_enabled: Option<bool>,
        encoding: Option<String>,
        dictionary_enabled: Option<bool>,
        compression: Option<String>,
        statistics_enabled: Option<String>,
        bloom_filter_fpp: Option<f64>,
        bloom_filter_ndv: Option<u64>,
    ) -> Self {
        Self {
            options: ParquetColumnOptions {
                bloom_filter_enabled,
                encoding,
                dictionary_enabled,
                compression,
                statistics_enabled,
                bloom_filter_fpp,
                bloom_filter_ndv,
                ..Default::default()
            },
        }
    }
}

/// A PyDataFrame is a representation of a logical plan and an API to compose statements.
/// Use it to build a plan and `.collect()` to execute the plan and collect the result.
/// The actual execution of a plan runs natively on Rust and Arrow on a multi-threaded environment.
#[pyclass(name = "DataFrame", module = "datafusion", subclass)]
#[derive(Clone)]
pub struct PyDataFrame {
    df: Arc<DataFrame>,

    // In IPython environment cache batches between __repr__ and _repr_html_ calls.
    batches: Option<(Vec<RecordBatch>, bool)>,
}

impl PyDataFrame {
    /// creates a new PyDataFrame
    pub fn new(df: DataFrame) -> Self {
        Self {
            df: Arc::new(df),
            batches: None,
        }
    }

    fn prepare_repr_string(&mut self, py: Python, as_html: bool) -> PyDataFusionResult<String> {
        // Get the Python formatter and config
        let PythonFormatter { formatter, config } = get_python_formatter_with_config(py)?;

        let should_cache = *is_ipython_env(py) && self.batches.is_none();
        let (batches, has_more) = match self.batches.take() {
            Some(b) => b,
            None => wait_for_future(
                py,
                collect_record_batches_to_display(self.df.as_ref().clone(), config),
            )??,
        };

        if batches.is_empty() {
            // This should not be reached, but do it for safety since we index into the vector below
            return Ok("No data to display".to_string());
        }

        let table_uuid = uuid::Uuid::new_v4().to_string();

        // Convert record batches to PyObject list
        let py_batches = batches
            .iter()
            .map(|rb| rb.to_pyarrow(py))
            .collect::<PyResult<Vec<PyObject>>>()?;

        let py_schema = self.schema().into_pyobject(py)?;

        let kwargs = pyo3::types::PyDict::new(py);
        let py_batches_list = PyList::new(py, py_batches.as_slice())?;
        kwargs.set_item("batches", py_batches_list)?;
        kwargs.set_item("schema", py_schema)?;
        kwargs.set_item("has_more", has_more)?;
        kwargs.set_item("table_uuid", table_uuid)?;

        let method_name = match as_html {
            true => "format_html",
            false => "format_str",
        };

        let html_result = formatter.call_method(method_name, (), Some(&kwargs))?;
        let html_str: String = html_result.extract()?;

        if should_cache {
            self.batches = Some((batches, has_more));
        }

        Ok(html_str)
    }
}

#[pymethods]
impl PyDataFrame {
    /// Enable selection for `df[col]`, `df[col1, col2, col3]`, and `df[[col1, col2, col3]]`
    fn __getitem__(&self, key: Bound<'_, PyAny>) -> PyDataFusionResult<Self> {
        if let Ok(key) = key.extract::<PyBackedStr>() {
            // df[col]
            self.select_columns(vec![key])
        } else if let Ok(tuple) = key.downcast::<PyTuple>() {
            // df[col1, col2, col3]
            let keys = tuple
                .iter()
                .map(|item| item.extract::<PyBackedStr>())
                .collect::<PyResult<Vec<PyBackedStr>>>()?;
            self.select_columns(keys)
        } else if let Ok(keys) = key.extract::<Vec<PyBackedStr>>() {
            // df[[col1, col2, col3]]
            self.select_columns(keys)
        } else {
            let message = "DataFrame can only be indexed by string index or indices".to_string();
            Err(PyDataFusionError::Common(message))
        }
    }

    fn __repr__(&mut self, py: Python) -> PyDataFusionResult<String> {
        self.prepare_repr_string(py, false)
    }

    #[staticmethod]
    #[expect(unused_variables)]
    fn default_str_repr<'py>(
        batches: Vec<Bound<'py, PyAny>>,
        schema: &Bound<'py, PyAny>,
        has_more: bool,
        table_uuid: &str,
    ) -> PyResult<String> {
        let batches = batches
            .into_iter()
            .map(|batch| RecordBatch::from_pyarrow_bound(&batch))
            .collect::<PyResult<Vec<RecordBatch>>>()?
            .into_iter()
            .filter(|batch| batch.num_rows() > 0)
            .collect::<Vec<_>>();

        if batches.is_empty() {
            return Ok("No data to display".to_owned());
        }

        let batches_as_displ =
            pretty::pretty_format_batches(&batches).map_err(py_datafusion_err)?;

        let additional_str = match has_more {
            true => "\nData truncated.",
            false => "",
        };

        Ok(format!("DataFrame()\n{batches_as_displ}{additional_str}"))
    }

    fn _repr_html_(&mut self, py: Python) -> PyDataFusionResult<String> {
        self.prepare_repr_string(py, true)
    }

    /// Calculate summary statistics for a DataFrame
    fn describe(&self, py: Python) -> PyDataFusionResult<Self> {
        let df = self.df.as_ref().clone();
        let stat_df = wait_for_future(py, df.describe())??;
        Ok(Self::new(stat_df))
    }

    /// Returns the schema from the logical plan
    fn schema(&self) -> PyArrowType<Schema> {
        PyArrowType(self.df.schema().into())
    }

    /// Convert this DataFrame into a Table that can be used in register_table
    /// By convention, into_... methods consume self and return the new object.
    /// Disabling the clippy lint, so we can use &self
    /// because we're working with Python bindings
    /// where objects are shared
    /// https://github.com/apache/datafusion-python/pull/1016#discussion_r1983239116
    /// - we have not decided on the table_provider approach yet
    #[allow(clippy::wrong_self_convention)]
    fn into_view(&self) -> PyDataFusionResult<PyTable> {
        // Call the underlying Rust DataFrame::into_view method.
        // Note that the Rust method consumes self; here we clone the inner Arc<DataFrame>
        // so that we don’t invalidate this PyDataFrame.
        let table_provider = self.df.as_ref().clone().into_view();
        let table_provider = PyTableProvider::new(table_provider);

        Ok(table_provider.as_table())
    }

    #[pyo3(signature = (*args))]
    fn select_columns(&self, args: Vec<PyBackedStr>) -> PyDataFusionResult<Self> {
        let args = args.iter().map(|s| s.as_ref()).collect::<Vec<&str>>();
        let df = self.df.as_ref().clone().select_columns(&args)?;
        Ok(Self::new(df))
    }

    #[pyo3(signature = (*args))]
    fn select(&self, args: Vec<PyExpr>) -> PyDataFusionResult<Self> {
        let expr: Vec<Expr> = args.into_iter().map(|e| e.into()).collect();
        let df = self.df.as_ref().clone().select(expr)?;
        Ok(Self::new(df))
    }

    #[pyo3(signature = (*args))]
    fn drop(&self, args: Vec<PyBackedStr>) -> PyDataFusionResult<Self> {
        let cols = args.iter().map(|s| s.as_ref()).collect::<Vec<&str>>();
        let df = self.df.as_ref().clone().drop_columns(&cols)?;
        Ok(Self::new(df))
    }

    fn filter(&self, predicate: PyExpr) -> PyDataFusionResult<Self> {
        let df = self.df.as_ref().clone().filter(predicate.into())?;
        Ok(Self::new(df))
    }

    fn with_column(&self, name: &str, expr: PyExpr) -> PyDataFusionResult<Self> {
        let df = self.df.as_ref().clone().with_column(name, expr.into())?;
        Ok(Self::new(df))
    }

    fn with_columns(&self, exprs: Vec<PyExpr>) -> PyDataFusionResult<Self> {
        let mut df = self.df.as_ref().clone();
        for expr in exprs {
            let expr: Expr = expr.into();
            let name = format!("{}", expr.schema_name());
            df = df.with_column(name.as_str(), expr)?
        }
        Ok(Self::new(df))
    }

    /// Rename one column by applying a new projection. This is a no-op if the column to be
    /// renamed does not exist.
    fn with_column_renamed(&self, old_name: &str, new_name: &str) -> PyDataFusionResult<Self> {
        let df = self
            .df
            .as_ref()
            .clone()
            .with_column_renamed(old_name, new_name)?;
        Ok(Self::new(df))
    }

    fn aggregate(&self, group_by: Vec<PyExpr>, aggs: Vec<PyExpr>) -> PyDataFusionResult<Self> {
        let group_by = group_by.into_iter().map(|e| e.into()).collect();
        let aggs = aggs.into_iter().map(|e| e.into()).collect();
        let df = self.df.as_ref().clone().aggregate(group_by, aggs)?;
        Ok(Self::new(df))
    }

    #[pyo3(signature = (*exprs))]
    fn sort(&self, exprs: Vec<PySortExpr>) -> PyDataFusionResult<Self> {
        let exprs = to_sort_expressions(exprs);
        let df = self.df.as_ref().clone().sort(exprs)?;
        Ok(Self::new(df))
    }

    #[pyo3(signature = (count, offset=0))]
    fn limit(&self, count: usize, offset: usize) -> PyDataFusionResult<Self> {
        let df = self.df.as_ref().clone().limit(offset, Some(count))?;
        Ok(Self::new(df))
    }

    /// Executes the plan, returning a list of `RecordBatch`es.
    /// Unless some order is specified in the plan, there is no
    /// guarantee of the order of the result.
    fn collect(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let batches = wait_for_future(py, self.df.as_ref().clone().collect())?
            .map_err(PyDataFusionError::from)?;
        // cannot use PyResult<Vec<RecordBatch>> return type due to
        // https://github.com/PyO3/pyo3/issues/1813
        batches.into_iter().map(|rb| rb.to_pyarrow(py)).collect()
    }

    /// Cache DataFrame.
    fn cache(&self, py: Python) -> PyDataFusionResult<Self> {
        let df = wait_for_future(py, self.df.as_ref().clone().cache())??;
        Ok(Self::new(df))
    }

    /// Executes this DataFrame and collects all results into a vector of vector of RecordBatch
    /// maintaining the input partitioning.
    fn collect_partitioned(&self, py: Python) -> PyResult<Vec<Vec<PyObject>>> {
        let batches = wait_for_future(py, self.df.as_ref().clone().collect_partitioned())?
            .map_err(PyDataFusionError::from)?;

        batches
            .into_iter()
            .map(|rbs| rbs.into_iter().map(|rb| rb.to_pyarrow(py)).collect())
            .collect()
    }

    /// Print the result, 20 lines by default
    #[pyo3(signature = (num=20))]
    fn show(&self, py: Python, num: usize) -> PyDataFusionResult<()> {
        let df = self.df.as_ref().clone().limit(0, Some(num))?;
        print_dataframe(py, df)
    }

    /// Filter out duplicate rows
    fn distinct(&self) -> PyDataFusionResult<Self> {
        let df = self.df.as_ref().clone().distinct()?;
        Ok(Self::new(df))
    }

    fn join(
        &self,
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

        let df = self.df.as_ref().clone().join(
            right.df.as_ref().clone(),
            join_type,
            &left_keys,
            &right_keys,
            None,
        )?;
        Ok(Self::new(df))
    }

    fn join_on(
        &self,
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
            .as_ref()
            .clone()
            .join_on(right.df.as_ref().clone(), join_type, exprs)?;
        Ok(Self::new(df))
    }

    /// Print the query plan
    #[pyo3(signature = (verbose=false, analyze=false))]
    fn explain(&self, py: Python, verbose: bool, analyze: bool) -> PyDataFusionResult<()> {
        let df = self.df.as_ref().clone().explain(verbose, analyze)?;
        print_dataframe(py, df)
    }

    /// Get the logical plan for this `DataFrame`
    fn logical_plan(&self) -> PyResult<PyLogicalPlan> {
        Ok(self.df.as_ref().clone().logical_plan().clone().into())
    }

    /// Get the optimized logical plan for this `DataFrame`
    fn optimized_logical_plan(&self) -> PyDataFusionResult<PyLogicalPlan> {
        Ok(self.df.as_ref().clone().into_optimized_plan()?.into())
    }

    /// Get the execution plan for this `DataFrame`
    fn execution_plan(&self, py: Python) -> PyDataFusionResult<PyExecutionPlan> {
        let plan = wait_for_future(py, self.df.as_ref().clone().create_physical_plan())??;
        Ok(plan.into())
    }

    /// Repartition a `DataFrame` based on a logical partitioning scheme.
    fn repartition(&self, num: usize) -> PyDataFusionResult<Self> {
        let new_df = self
            .df
            .as_ref()
            .clone()
            .repartition(Partitioning::RoundRobinBatch(num))?;
        Ok(Self::new(new_df))
    }

    /// Repartition a `DataFrame` based on a logical partitioning scheme.
    #[pyo3(signature = (*args, num))]
    fn repartition_by_hash(&self, args: Vec<PyExpr>, num: usize) -> PyDataFusionResult<Self> {
        let expr = args.into_iter().map(|py_expr| py_expr.into()).collect();
        let new_df = self
            .df
            .as_ref()
            .clone()
            .repartition(Partitioning::Hash(expr, num))?;
        Ok(Self::new(new_df))
    }

    /// Calculate the union of two `DataFrame`s, preserving duplicate rows.The
    /// two `DataFrame`s must have exactly the same schema
    #[pyo3(signature = (py_df, distinct=false))]
    fn union(&self, py_df: PyDataFrame, distinct: bool) -> PyDataFusionResult<Self> {
        let new_df = if distinct {
            self.df
                .as_ref()
                .clone()
                .union_distinct(py_df.df.as_ref().clone())?
        } else {
            self.df.as_ref().clone().union(py_df.df.as_ref().clone())?
        };

        Ok(Self::new(new_df))
    }

    /// Calculate the distinct union of two `DataFrame`s.  The
    /// two `DataFrame`s must have exactly the same schema
    fn union_distinct(&self, py_df: PyDataFrame) -> PyDataFusionResult<Self> {
        let new_df = self
            .df
            .as_ref()
            .clone()
            .union_distinct(py_df.df.as_ref().clone())?;
        Ok(Self::new(new_df))
    }

    #[pyo3(signature = (column, preserve_nulls=true))]
    fn unnest_column(&self, column: &str, preserve_nulls: bool) -> PyDataFusionResult<Self> {
        // TODO: expose RecursionUnnestOptions
        // REF: https://github.com/apache/datafusion/pull/11577
        let unnest_options = UnnestOptions::default().with_preserve_nulls(preserve_nulls);
        let df = self
            .df
            .as_ref()
            .clone()
            .unnest_columns_with_options(&[column], unnest_options)?;
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
            .as_ref()
            .clone()
            .unnest_columns_with_options(&cols, unnest_options)?;
        Ok(Self::new(df))
    }

    /// Calculate the intersection of two `DataFrame`s.  The two `DataFrame`s must have exactly the same schema
    fn intersect(&self, py_df: PyDataFrame) -> PyDataFusionResult<Self> {
        let new_df = self
            .df
            .as_ref()
            .clone()
            .intersect(py_df.df.as_ref().clone())?;
        Ok(Self::new(new_df))
    }

    /// Calculate the exception of two `DataFrame`s.  The two `DataFrame`s must have exactly the same schema
    fn except_all(&self, py_df: PyDataFrame) -> PyDataFusionResult<Self> {
        let new_df = self.df.as_ref().clone().except(py_df.df.as_ref().clone())?;
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
            self.df.as_ref().clone().write_csv(
                path,
                DataFrameWriteOptions::new(),
                Some(csv_options),
            ),
        )??;
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
            self.df.as_ref().clone().write_parquet(
                path,
                DataFrameWriteOptions::new(),
                Option::from(options),
            ),
        )??;
        Ok(())
    }

    /// Write a `DataFrame` to a Parquet file, using advanced options.
    fn write_parquet_with_options(
        &self,
        path: &str,
        options: PyParquetWriterOptions,
        column_specific_options: HashMap<String, PyParquetColumnOptions>,
        py: Python,
    ) -> PyDataFusionResult<()> {
        let table_options = TableParquetOptions {
            global: options.options,
            column_specific_options: column_specific_options
                .into_iter()
                .map(|(k, v)| (k, v.options))
                .collect(),
            ..Default::default()
        };

        wait_for_future(
            py,
            self.df.as_ref().clone().write_parquet(
                path,
                DataFrameWriteOptions::new(),
                Option::from(table_options),
            ),
        )??;
        Ok(())
    }

    /// Executes a query and writes the results to a partitioned JSON file.
    fn write_json(&self, path: &str, py: Python) -> PyDataFusionResult<()> {
        wait_for_future(
            py,
            self.df
                .as_ref()
                .clone()
                .write_json(path, DataFrameWriteOptions::new(), None),
        )??;
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
        let mut batches = wait_for_future(py, self.df.as_ref().clone().collect())??;
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
        let df = self.df.as_ref().clone();
        let fut: JoinHandle<datafusion::common::Result<SendableRecordBatchStream>> =
            rt.spawn(async move { df.execute_stream().await });
        let stream = wait_for_future(py, async { fut.await.map_err(to_datafusion_err) })???;
        Ok(PyRecordBatchStream::new(stream))
    }

    fn execute_stream_partitioned(&self, py: Python) -> PyResult<Vec<PyRecordBatchStream>> {
        // create a Tokio runtime to run the async code
        let rt = &get_tokio_runtime().0;
        let df = self.df.as_ref().clone();
        let fut: JoinHandle<datafusion::common::Result<Vec<SendableRecordBatchStream>>> =
            rt.spawn(async move { df.execute_stream_partitioned().await });
        let stream = wait_for_future(py, async { fut.await.map_err(to_datafusion_err) })?
            .map_err(py_datafusion_err)?
            .map_err(py_datafusion_err)?;

        Ok(stream.into_iter().map(PyRecordBatchStream::new).collect())
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
        Ok(wait_for_future(py, self.df.as_ref().clone().count())??)
    }

    /// Fill null values with a specified value for specific columns
    #[pyo3(signature = (value, columns=None))]
    fn fill_null(
        &self,
        value: PyObject,
        columns: Option<Vec<PyBackedStr>>,
        py: Python,
    ) -> PyDataFusionResult<Self> {
        let scalar_value = py_obj_to_scalar_value(py, value)?;

        let cols = match columns {
            Some(col_names) => col_names.iter().map(|c| c.to_string()).collect(),
            None => Vec::new(), // Empty vector means fill null for all columns
        };

        let df = self.df.as_ref().clone().fill_null(scalar_value, cols)?;
        Ok(Self::new(df))
    }
}

/// Print DataFrame
fn print_dataframe(py: Python, df: DataFrame) -> PyDataFusionResult<()> {
    // Get string representation of record batches
    let batches = wait_for_future(py, df.collect())??;
    let batches_as_string = pretty::pretty_format_batches(&batches);
    let result = match batches_as_string {
        Ok(batch) => format!("DataFrame()\n{batch}"),
        Err(err) => format!("Error: {:?}", err.to_string()),
    };

    // Import the Python 'builtins' module to access the print function
    // Note that println! does not print to the Python debug console and is not visible in notebooks for instance
    let print = py.import("builtins")?.getattr("print")?;
    print.call1((result,))?;
    Ok(())
}

fn project_schema(from_schema: Schema, to_schema: Schema) -> Result<Schema, ArrowError> {
    let merged_schema = Schema::try_merge(vec![from_schema, to_schema.clone()])?;

    let project_indices: Vec<usize> = to_schema
        .fields
        .iter()
        .map(|field| field.name())
        .filter_map(|field_name| merged_schema.index_of(field_name).ok())
        .collect();

    merged_schema.project(&project_indices)
}

fn record_batch_into_schema(
    record_batch: RecordBatch,
    schema: &Schema,
) -> Result<RecordBatch, ArrowError> {
    let schema = Arc::new(schema.clone());
    let base_schema = record_batch.schema();
    if base_schema.fields().is_empty() {
        // Nothing to project
        return Ok(RecordBatch::new_empty(schema));
    }

    let array_size = record_batch.column(0).len();
    let mut data_arrays = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let desired_data_type = field.data_type();
        if let Some(original_data) = record_batch.column_by_name(field.name()) {
            let original_data_type = original_data.data_type();

            if can_cast_types(original_data_type, desired_data_type) {
                data_arrays.push(arrow::compute::kernels::cast(
                    original_data,
                    desired_data_type,
                )?);
            } else if field.is_nullable() {
                data_arrays.push(new_null_array(desired_data_type, array_size));
            } else {
                return Err(ArrowError::CastError(format!("Attempting to cast to non-nullable and non-castable field {} during schema projection.", field.name())));
            }
        } else {
            if !field.is_nullable() {
                return Err(ArrowError::CastError(format!(
                    "Attempting to set null to non-nullable field {} during schema projection.",
                    field.name()
                )));
            }
            data_arrays.push(new_null_array(desired_data_type, array_size));
        }
    }

    RecordBatch::try_new(schema, data_arrays)
}

/// This is a helper function to return the first non-empty record batch from executing a DataFrame.
/// It additionally returns a bool, which indicates if there are more record batches available.
/// We do this so we can determine if we should indicate to the user that the data has been
/// truncated. This collects until we have achived both of these two conditions
///
/// - We have collected our minimum number of rows
/// - We have reached our limit, either data size or maximum number of rows
///
/// Otherwise it will return when the stream has exhausted. If you want a specific number of
/// rows, set min_rows == max_rows.
async fn collect_record_batches_to_display(
    df: DataFrame,
    config: FormatterConfig,
) -> Result<(Vec<RecordBatch>, bool), DataFusionError> {
    let FormatterConfig {
        max_bytes,
        min_rows,
        repr_rows,
    } = config;

    let partitioned_stream = df.execute_stream_partitioned().await?;
    let mut stream = futures::stream::iter(partitioned_stream).flatten();
    let mut size_estimate_so_far = 0;
    let mut rows_so_far = 0;
    let mut record_batches = Vec::default();
    let mut has_more = false;

    // ensure minimum rows even if memory/row limits are hit
    while (size_estimate_so_far < max_bytes && rows_so_far < repr_rows) || rows_so_far < min_rows {
        let mut rb = match stream.next().await {
            None => {
                break;
            }
            Some(Ok(r)) => r,
            Some(Err(e)) => return Err(e),
        };

        let mut rows_in_rb = rb.num_rows();
        if rows_in_rb > 0 {
            size_estimate_so_far += rb.get_array_memory_size();

            if size_estimate_so_far > max_bytes {
                let ratio = max_bytes as f32 / size_estimate_so_far as f32;
                let total_rows = rows_in_rb + rows_so_far;

                let mut reduced_row_num = (total_rows as f32 * ratio).round() as usize;
                if reduced_row_num < min_rows {
                    reduced_row_num = min_rows.min(total_rows);
                }

                let limited_rows_this_rb = reduced_row_num - rows_so_far;
                if limited_rows_this_rb < rows_in_rb {
                    rows_in_rb = limited_rows_this_rb;
                    rb = rb.slice(0, limited_rows_this_rb);
                    has_more = true;
                }
            }

            if rows_in_rb + rows_so_far > repr_rows {
                rb = rb.slice(0, repr_rows - rows_so_far);
                has_more = true;
            }

            rows_so_far += rb.num_rows();
            record_batches.push(rb);
        }
    }

    if record_batches.is_empty() {
        return Ok((Vec::default(), false));
    }

    if !has_more {
        // Data was not already truncated, so check to see if more record batches remain
        has_more = match stream.try_next().await {
            Ok(None) => false, // reached end
            Ok(Some(_)) => true,
            Err(_) => false, // Stream disconnected
        };
    }

    Ok((record_batches, has_more))
}
