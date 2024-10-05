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

use arrow::array::{new_null_array, RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow::compute::can_cast_types;
use arrow::error::ArrowError;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::pyarrow::{PyArrowType, ToPyArrow};
use datafusion::arrow::util::pretty;
use datafusion::common::UnnestOptions;
use datafusion::config::{CsvOptions, TableParquetOptions};
use datafusion::dataframe::{DataFrame, DataFrameWriteOptions};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use datafusion::prelude::*;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyCapsule, PyTuple, PyTupleMethods};
use tokio::task::JoinHandle;

use crate::errors::py_datafusion_err;
use crate::expr::sort_expr::to_sort_expressions;
use crate::physical_plan::PyExecutionPlan;
use crate::record_batch::PyRecordBatchStream;
use crate::sql::logical::PyLogicalPlan;
use crate::utils::{get_tokio_runtime, wait_for_future};
use crate::{
    errors::DataFusionError,
    expr::{sort_expr::PySortExpr, PyExpr},
};

/// A PyDataFrame is a representation of a logical plan and an API to compose statements.
/// Use it to build a plan and `.collect()` to execute the plan and collect the result.
/// The actual execution of a plan runs natively on Rust and Arrow on a multi-threaded environment.
#[pyclass(name = "DataFrame", module = "datafusion", subclass)]
#[derive(Clone)]
pub struct PyDataFrame {
    df: Arc<DataFrame>,
}

impl PyDataFrame {
    /// creates a new PyDataFrame
    pub fn new(df: DataFrame) -> Self {
        Self { df: Arc::new(df) }
    }
}

#[pymethods]
impl PyDataFrame {
    /// Enable selection for `df[col]`, `df[col1, col2, col3]`, and `df[[col1, col2, col3]]`
    fn __getitem__(&self, key: Bound<'_, PyAny>) -> PyResult<Self> {
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
            let message = "DataFrame can only be indexed by string index or indices";
            Err(PyTypeError::new_err(message))
        }
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        let df = self.df.as_ref().clone().limit(0, Some(10))?;
        let batches = wait_for_future(py, df.collect())?;
        let batches_as_string = pretty::pretty_format_batches(&batches);
        match batches_as_string {
            Ok(batch) => Ok(format!("DataFrame()\n{batch}")),
            Err(err) => Ok(format!("Error: {:?}", err.to_string())),
        }
    }

    fn _repr_html_(&self, py: Python) -> PyResult<String> {
        let mut html_str = "<table border='1'>\n".to_string();

        let df = self.df.as_ref().clone().limit(0, Some(10))?;
        let batches = wait_for_future(py, df.collect())?;

        if batches.is_empty() {
            html_str.push_str("</table>\n");
            return Ok(html_str);
        }

        let schema = batches[0].schema();

        let mut header = Vec::new();
        for field in schema.fields() {
            header.push(format!("<th>{}</td>", field.name()));
        }
        let header_str = header.join("");
        html_str.push_str(&format!("<tr>{}</tr>\n", header_str));

        for batch in batches {
            let formatters = batch
                .columns()
                .iter()
                .map(|c| ArrayFormatter::try_new(c.as_ref(), &FormatOptions::default()))
                .map(|c| {
                    c.map_err(|e| PyValueError::new_err(format!("Error: {:?}", e.to_string())))
                })
                .collect::<Result<Vec<_>, _>>()?;

            for row in 0..batch.num_rows() {
                let mut cells = Vec::new();
                for formatter in &formatters {
                    cells.push(format!("<td>{}</td>", formatter.value(row)));
                }
                let row_str = cells.join("");
                html_str.push_str(&format!("<tr>{}</tr>\n", row_str));
            }
        }

        html_str.push_str("</table>\n");

        Ok(html_str)
    }

    /// Calculate summary statistics for a DataFrame
    fn describe(&self, py: Python) -> PyResult<Self> {
        let df = self.df.as_ref().clone();
        let stat_df = wait_for_future(py, df.describe())?;
        Ok(Self::new(stat_df))
    }

    /// Returns the schema from the logical plan
    fn schema(&self) -> PyArrowType<Schema> {
        PyArrowType(self.df.schema().into())
    }

    #[pyo3(signature = (*args))]
    fn select_columns(&self, args: Vec<PyBackedStr>) -> PyResult<Self> {
        let args = args.iter().map(|s| s.as_ref()).collect::<Vec<&str>>();
        let df = self.df.as_ref().clone().select_columns(&args)?;
        Ok(Self::new(df))
    }

    #[pyo3(signature = (*args))]
    fn select(&self, args: Vec<PyExpr>) -> PyResult<Self> {
        let expr = args.into_iter().map(|e| e.into()).collect();
        let df = self.df.as_ref().clone().select(expr)?;
        Ok(Self::new(df))
    }

    fn filter(&self, predicate: PyExpr) -> PyResult<Self> {
        let df = self.df.as_ref().clone().filter(predicate.into())?;
        Ok(Self::new(df))
    }

    fn with_column(&self, name: &str, expr: PyExpr) -> PyResult<Self> {
        let df = self.df.as_ref().clone().with_column(name, expr.into())?;
        Ok(Self::new(df))
    }

    /// Rename one column by applying a new projection. This is a no-op if the column to be
    /// renamed does not exist.
    fn with_column_renamed(&self, old_name: &str, new_name: &str) -> PyResult<Self> {
        let df = self
            .df
            .as_ref()
            .clone()
            .with_column_renamed(old_name, new_name)?;
        Ok(Self::new(df))
    }

    fn aggregate(&self, group_by: Vec<PyExpr>, aggs: Vec<PyExpr>) -> PyResult<Self> {
        let group_by = group_by.into_iter().map(|e| e.into()).collect();
        let aggs = aggs.into_iter().map(|e| e.into()).collect();
        let df = self.df.as_ref().clone().aggregate(group_by, aggs)?;
        Ok(Self::new(df))
    }

    #[pyo3(signature = (*exprs))]
    fn sort(&self, exprs: Vec<PySortExpr>) -> PyResult<Self> {
        let exprs = to_sort_expressions(exprs);
        let df = self.df.as_ref().clone().sort(exprs)?;
        Ok(Self::new(df))
    }

    #[pyo3(signature = (count, offset=0))]
    fn limit(&self, count: usize, offset: usize) -> PyResult<Self> {
        let df = self.df.as_ref().clone().limit(offset, Some(count))?;
        Ok(Self::new(df))
    }

    /// Executes the plan, returning a list of `RecordBatch`es.
    /// Unless some order is specified in the plan, there is no
    /// guarantee of the order of the result.
    fn collect(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let batches = wait_for_future(py, self.df.as_ref().clone().collect())?;
        // cannot use PyResult<Vec<RecordBatch>> return type due to
        // https://github.com/PyO3/pyo3/issues/1813
        batches.into_iter().map(|rb| rb.to_pyarrow(py)).collect()
    }

    /// Cache DataFrame.
    fn cache(&self, py: Python) -> PyResult<Self> {
        let df = wait_for_future(py, self.df.as_ref().clone().cache())?;
        Ok(Self::new(df))
    }

    /// Executes this DataFrame and collects all results into a vector of vector of RecordBatch
    /// maintaining the input partitioning.
    fn collect_partitioned(&self, py: Python) -> PyResult<Vec<Vec<PyObject>>> {
        let batches = wait_for_future(py, self.df.as_ref().clone().collect_partitioned())?;

        batches
            .into_iter()
            .map(|rbs| rbs.into_iter().map(|rb| rb.to_pyarrow(py)).collect())
            .collect()
    }

    /// Print the result, 20 lines by default
    #[pyo3(signature = (num=20))]
    fn show(&self, py: Python, num: usize) -> PyResult<()> {
        let df = self.df.as_ref().clone().limit(0, Some(num))?;
        print_dataframe(py, df)
    }

    /// Filter out duplicate rows
    fn distinct(&self) -> PyResult<Self> {
        let df = self.df.as_ref().clone().distinct()?;
        Ok(Self::new(df))
    }

    fn join(
        &self,
        right: PyDataFrame,
        join_keys: (Vec<PyBackedStr>, Vec<PyBackedStr>),
        how: &str,
    ) -> PyResult<Self> {
        let join_type = match how {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "full" => JoinType::Full,
            "semi" => JoinType::LeftSemi,
            "anti" => JoinType::LeftAnti,
            how => {
                return Err(DataFusionError::Common(format!(
                    "The join type {how} does not exist or is not implemented"
                ))
                .into());
            }
        };

        let left_keys = join_keys
            .0
            .iter()
            .map(|s| s.as_ref())
            .collect::<Vec<&str>>();
        let right_keys = join_keys
            .1
            .iter()
            .map(|s| s.as_ref())
            .collect::<Vec<&str>>();

        let df = self.df.as_ref().clone().join(
            right.df.as_ref().clone(),
            join_type,
            &left_keys,
            &right_keys,
            None,
        )?;
        Ok(Self::new(df))
    }

    /// Print the query plan
    #[pyo3(signature = (verbose=false, analyze=false))]
    fn explain(&self, py: Python, verbose: bool, analyze: bool) -> PyResult<()> {
        let df = self.df.as_ref().clone().explain(verbose, analyze)?;
        print_dataframe(py, df)
    }

    /// Get the logical plan for this `DataFrame`
    fn logical_plan(&self) -> PyResult<PyLogicalPlan> {
        Ok(self.df.as_ref().clone().logical_plan().clone().into())
    }

    /// Get the optimized logical plan for this `DataFrame`
    fn optimized_logical_plan(&self) -> PyResult<PyLogicalPlan> {
        Ok(self.df.as_ref().clone().into_optimized_plan()?.into())
    }

    /// Get the execution plan for this `DataFrame`
    fn execution_plan(&self, py: Python) -> PyResult<PyExecutionPlan> {
        let plan = wait_for_future(py, self.df.as_ref().clone().create_physical_plan())?;
        Ok(plan.into())
    }

    /// Repartition a `DataFrame` based on a logical partitioning scheme.
    fn repartition(&self, num: usize) -> PyResult<Self> {
        let new_df = self
            .df
            .as_ref()
            .clone()
            .repartition(Partitioning::RoundRobinBatch(num))?;
        Ok(Self::new(new_df))
    }

    /// Repartition a `DataFrame` based on a logical partitioning scheme.
    #[pyo3(signature = (*args, num))]
    fn repartition_by_hash(&self, args: Vec<PyExpr>, num: usize) -> PyResult<Self> {
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
    fn union(&self, py_df: PyDataFrame, distinct: bool) -> PyResult<Self> {
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
    fn union_distinct(&self, py_df: PyDataFrame) -> PyResult<Self> {
        let new_df = self
            .df
            .as_ref()
            .clone()
            .union_distinct(py_df.df.as_ref().clone())?;
        Ok(Self::new(new_df))
    }

    #[pyo3(signature = (column, preserve_nulls=true))]
    fn unnest_column(&self, column: &str, preserve_nulls: bool) -> PyResult<Self> {
        let unnest_options = UnnestOptions { preserve_nulls };
        let df = self
            .df
            .as_ref()
            .clone()
            .unnest_columns_with_options(&[column], unnest_options)?;
        Ok(Self::new(df))
    }

    #[pyo3(signature = (columns, preserve_nulls=true))]
    fn unnest_columns(&self, columns: Vec<String>, preserve_nulls: bool) -> PyResult<Self> {
        let unnest_options = UnnestOptions { preserve_nulls };
        let cols = columns.iter().map(|s| s.as_ref()).collect::<Vec<&str>>();
        let df = self
            .df
            .as_ref()
            .clone()
            .unnest_columns_with_options(&cols, unnest_options)?;
        Ok(Self::new(df))
    }

    /// Calculate the intersection of two `DataFrame`s.  The two `DataFrame`s must have exactly the same schema
    fn intersect(&self, py_df: PyDataFrame) -> PyResult<Self> {
        let new_df = self
            .df
            .as_ref()
            .clone()
            .intersect(py_df.df.as_ref().clone())?;
        Ok(Self::new(new_df))
    }

    /// Calculate the exception of two `DataFrame`s.  The two `DataFrame`s must have exactly the same schema
    fn except_all(&self, py_df: PyDataFrame) -> PyResult<Self> {
        let new_df = self.df.as_ref().clone().except(py_df.df.as_ref().clone())?;
        Ok(Self::new(new_df))
    }

    /// Write a `DataFrame` to a CSV file.
    fn write_csv(&self, path: &str, with_header: bool, py: Python) -> PyResult<()> {
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
        )?;
        Ok(())
    }

    /// Write a `DataFrame` to a Parquet file.
    #[pyo3(signature = (
        path,
        compression="uncompressed",
        compression_level=None
        ))]
    fn write_parquet(
        &self,
        path: &str,
        compression: &str,
        compression_level: Option<u32>,
        py: Python,
    ) -> PyResult<()> {
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
            "lz0" => Compression::LZO,
            "lz4" => Compression::LZ4,
            "lz4_raw" => Compression::LZ4_RAW,
            "uncompressed" => Compression::UNCOMPRESSED,
            _ => {
                return Err(PyValueError::new_err(format!(
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
        )?;
        Ok(())
    }

    /// Executes a query and writes the results to a partitioned JSON file.
    fn write_json(&self, path: &str, py: Python) -> PyResult<()> {
        wait_for_future(
            py,
            self.df
                .as_ref()
                .clone()
                .write_json(path, DataFrameWriteOptions::new(), None),
        )?;
        Ok(())
    }

    /// Convert to Arrow Table
    /// Collect the batches and pass to Arrow Table
    fn to_arrow_table(&self, py: Python<'_>) -> PyResult<PyObject> {
        let batches = self.collect(py)?.to_object(py);
        let schema: PyObject = self.schema().into_py(py);

        // Instantiate pyarrow Table object and use its from_batches method
        let table_class = py.import_bound("pyarrow")?.getattr("Table")?;
        let args = PyTuple::new_bound(py, &[batches, schema]);
        let table: PyObject = table_class.call_method1("from_batches", args)?.into();
        Ok(table)
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &'py mut self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let mut batches = wait_for_future(py, self.df.as_ref().clone().collect())?;
        let mut schema: Schema = self.df.schema().to_owned().into();

        if let Some(schema_capsule) = requested_schema {
            validate_pycapsule(&schema_capsule, "arrow_schema")?;

            let schema_ptr = unsafe { schema_capsule.reference::<FFI_ArrowSchema>() };
            let desired_schema = Schema::try_from(schema_ptr).map_err(DataFusionError::from)?;

            schema = project_schema(schema, desired_schema).map_err(DataFusionError::ArrowError)?;

            batches = batches
                .into_iter()
                .map(|record_batch| record_batch_into_schema(record_batch, &schema))
                .collect::<Result<Vec<RecordBatch>, ArrowError>>()
                .map_err(DataFusionError::ArrowError)?;
        }

        let batches_wrapped = batches.into_iter().map(Ok);

        let reader = RecordBatchIterator::new(batches_wrapped, Arc::new(schema));
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(reader);

        let ffi_stream = FFI_ArrowArrayStream::new(reader);
        let stream_capsule_name = CString::new("arrow_array_stream").unwrap();
        PyCapsule::new_bound(py, ffi_stream, Some(stream_capsule_name))
    }

    fn execute_stream(&self, py: Python) -> PyResult<PyRecordBatchStream> {
        // create a Tokio runtime to run the async code
        let rt = &get_tokio_runtime().0;
        let df = self.df.as_ref().clone();
        let fut: JoinHandle<datafusion::common::Result<SendableRecordBatchStream>> =
            rt.spawn(async move { df.execute_stream().await });
        let stream = wait_for_future(py, fut).map_err(py_datafusion_err)?;
        Ok(PyRecordBatchStream::new(stream?))
    }

    fn execute_stream_partitioned(&self, py: Python) -> PyResult<Vec<PyRecordBatchStream>> {
        // create a Tokio runtime to run the async code
        let rt = &get_tokio_runtime().0;
        let df = self.df.as_ref().clone();
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
        let dataframe = py.import_bound("polars")?.getattr("DataFrame")?;
        let args = PyTuple::new_bound(py, &[table]);
        let result: PyObject = dataframe.call1(args)?.into();
        Ok(result)
    }

    // Executes this DataFrame to get the total number of rows.
    fn count(&self, py: Python) -> PyResult<usize> {
        Ok(wait_for_future(py, self.df.as_ref().clone().count())?)
    }
}

/// Print DataFrame
fn print_dataframe(py: Python, df: DataFrame) -> PyResult<()> {
    // Get string representation of record batches
    let batches = wait_for_future(py, df.collect())?;
    let batches_as_string = pretty::pretty_format_batches(&batches);
    let result = match batches_as_string {
        Ok(batch) => format!("DataFrame()\n{batch}"),
        Err(err) => format!("Error: {:?}", err.to_string()),
    };

    // Import the Python 'builtins' module to access the print function
    // Note that println! does not print to the Python debug console and is not visible in notebooks for instance
    let print = py.import_bound("builtins")?.getattr("print")?;
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
    if base_schema.fields().len() == 0 {
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

fn validate_pycapsule(capsule: &Bound<PyCapsule>, name: &str) -> PyResult<()> {
    let capsule_name = capsule.name()?;
    if capsule_name.is_none() {
        return Err(PyValueError::new_err(
            "Expected schema PyCapsule to have name set.",
        ));
    }

    let capsule_name = capsule_name.unwrap().to_str()?;
    if capsule_name != name {
        return Err(PyValueError::new_err(format!(
            "Expected name '{}' in PyCapsule, instead got '{}'",
            name, capsule_name
        )));
    }

    Ok(())
}
