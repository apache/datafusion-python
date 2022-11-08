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

use crate::utils::wait_for_future;
use crate::{errors::DataFusionError, expression::PyExpr};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::pyarrow::{PyArrowConvert, PyArrowException, PyArrowType};
use datafusion::arrow::util::pretty;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::*;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use std::sync::Arc;

/// A PyDataFrame is a representation of a logical plan and an API to compose statements.
/// Use it to build a plan and `.collect()` to execute the plan and collect the result.
/// The actual execution of a plan runs natively on Rust and Arrow on a multi-threaded environment.
#[pyclass(name = "DataFrame", module = "datafusion", subclass)]
#[derive(Clone)]
pub(crate) struct PyDataFrame {
    df: Arc<DataFrame>,
}

impl PyDataFrame {
    /// creates a new PyDataFrame
    pub fn new(df: Arc<DataFrame>) -> Self {
        Self { df }
    }
}

#[pymethods]
impl PyDataFrame {
    fn __getitem__(&self, key: PyObject) -> PyResult<Self> {
        Python::with_gil(|py| {
            if let Ok(key) = key.extract::<&str>(py) {
                self.select_columns(vec![key])
            } else if let Ok(tuple) = key.extract::<&PyTuple>(py) {
                let keys = tuple
                    .iter()
                    .map(|item| item.extract::<&str>())
                    .collect::<PyResult<Vec<&str>>>()?;
                self.select_columns(keys)
            } else if let Ok(keys) = key.extract::<Vec<&str>>(py) {
                self.select_columns(keys)
            } else {
                let message = "DataFrame can only be indexed by string index or indices";
                Err(PyTypeError::new_err(message))
            }
        })
    }

    /// Returns the schema from the logical plan
    fn schema(&self) -> PyArrowType<Schema> {
        PyArrowType(self.df.schema().into())
    }

    #[args(args = "*")]
    fn select_columns(&self, args: Vec<&str>) -> PyResult<Self> {
        let df = self.df.select_columns(&args)?;
        Ok(Self::new(df))
    }

    #[args(args = "*")]
    fn select(&self, args: Vec<PyExpr>) -> PyResult<Self> {
        let expr = args.into_iter().map(|e| e.into()).collect();
        let df = self.df.select(expr)?;
        Ok(Self::new(df))
    }

    fn filter(&self, predicate: PyExpr) -> PyResult<Self> {
        let df = self.df.filter(predicate.into())?;
        Ok(Self::new(df))
    }

    fn with_column(&self, name: &str, expr: PyExpr) -> PyResult<Self> {
        let df = self.df.with_column(name, expr.into())?;
        Ok(Self::new(df))
    }

    /// Rename one column by applying a new projection. This is a no-op if the column to be
    /// renamed does not exist.
    fn with_column_renamed(&self, old_name: &str, new_name: &str) -> PyResult<Self> {
        let df = self.df.with_column_renamed(old_name, new_name)?;
        Ok(Self::new(df))
    }

    fn aggregate(&self, group_by: Vec<PyExpr>, aggs: Vec<PyExpr>) -> PyResult<Self> {
        let group_by = group_by.into_iter().map(|e| e.into()).collect();
        let aggs = aggs.into_iter().map(|e| e.into()).collect();
        let df = self.df.aggregate(group_by, aggs)?;
        Ok(Self::new(df))
    }

    #[args(exprs = "*")]
    fn sort(&self, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let exprs = exprs.into_iter().map(|e| e.into()).collect();
        let df = self.df.sort(exprs)?;
        Ok(Self::new(df))
    }

    fn limit(&self, count: usize) -> PyResult<Self> {
        let df = self.df.limit(0, Some(count))?;
        Ok(Self::new(df))
    }

    /// Executes the plan, returning a list of `RecordBatch`es.
    /// Unless some order is specified in the plan, there is no
    /// guarantee of the order of the result.
    fn collect(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let batches = wait_for_future(py, self.df.collect())?;
        // cannot use PyResult<Vec<RecordBatch>> return type due to
        // https://github.com/PyO3/pyo3/issues/1813
        batches.into_iter().map(|rb| rb.to_pyarrow(py)).collect()
    }

    /// Cache DataFrame.
    fn cache(&self, py: Python) -> PyResult<Self> {
        let df = wait_for_future(py, self.df.cache())?;
        Ok(Self::new(df))
    }

    /// Executes this DataFrame and collects all results into a vector of vector of RecordBatch
    /// maintaining the input partitioning.
    fn collect_partitioned(&self, py: Python) -> PyResult<Vec<Vec<PyObject>>> {
        let batches = wait_for_future(py, self.df.collect_partitioned())?;

        batches
            .into_iter()
            .map(|rbs| rbs.into_iter().map(|rb| rb.to_pyarrow(py)).collect())
            .collect()
    }

    /// Print the result, 20 lines by default
    #[args(num = "20")]
    fn show(&self, py: Python, num: usize) -> PyResult<()> {
        let df = self.df.limit(0, Some(num))?;
        let batches = wait_for_future(py, df.collect())?;
        pretty::print_batches(&batches).map_err(|err| PyArrowException::new_err(err.to_string()))
    }

    /// Filter out duplicate rows
    fn distinct(&self) -> PyResult<Self> {
        let df = self.df.distinct()?;
        Ok(Self::new(df))
    }

    fn join(
        &self,
        right: PyDataFrame,
        join_keys: (Vec<&str>, Vec<&str>),
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
                    "The join type {} does not exist or is not implemented",
                    how
                ))
                .into());
            }
        };

        let df = self
            .df
            .join(right.df, join_type, &join_keys.0, &join_keys.1, None)?;
        Ok(Self::new(df))
    }

    /// Print the query plan
    #[args(verbose = false, analyze = false)]
    fn explain(&self, py: Python, verbose: bool, analyze: bool) -> PyResult<()> {
        let df = self.df.explain(verbose, analyze)?;
        let batches = wait_for_future(py, df.collect())?;
        pretty::print_batches(&batches).map_err(|err| PyArrowException::new_err(err.to_string()))
    }

    /// Repartition a `DataFrame` based on a logical partitioning scheme.
    fn repartition(&self, num: usize) -> PyResult<Self> {
        let new_df = self.df.repartition(Partitioning::RoundRobinBatch(num))?;
        Ok(Self::new(new_df))
    }

    /// Repartition a `DataFrame` based on a logical partitioning scheme.
    #[args(args = "*", num)]
    fn repartition_by_hash(&self, args: Vec<PyExpr>, num: usize) -> PyResult<Self> {
        let expr = args.into_iter().map(|py_expr| py_expr.into()).collect();
        let new_df = self.df.repartition(Partitioning::Hash(expr, num))?;
        Ok(Self::new(new_df))
    }

    /// Calculate the union of two `DataFrame`s, preserving duplicate rows.The
    /// two `DataFrame`s must have exactly the same schema
    #[args(distinct = false)]
    fn union(&self, py_df: PyDataFrame, distinct: bool) -> PyResult<Self> {
        let new_df = if distinct {
            self.df.union_distinct(py_df.df)?
        } else {
            self.df.union(py_df.df)?
        };

        Ok(Self::new(new_df))
    }

    /// Calculate the distinct union of two `DataFrame`s.  The
    /// two `DataFrame`s must have exactly the same schema
    fn union_distinct(&self, py_df: PyDataFrame) -> PyResult<Self> {
        let new_df = self.df.union_distinct(py_df.df)?;
        Ok(Self::new(new_df))
    }

    /// Calculate the intersection of two `DataFrame`s.  The two `DataFrame`s must have exactly the same schema
    fn intersect(&self, py_df: PyDataFrame) -> PyResult<Self> {
        let new_df = self.df.intersect(py_df.df)?;
        Ok(Self::new(new_df))
    }

    /// Calculate the exception of two `DataFrame`s.  The two `DataFrame`s must have exactly the same schema
    fn except_all(&self, py_df: PyDataFrame) -> PyResult<Self> {
        let new_df = self.df.except(py_df.df)?;
        Ok(Self::new(new_df))
    }

    /// Write a `DataFrame` to a CSV file.
    fn write_csv(&self, path: &str, py: Python) -> PyResult<()> {
        wait_for_future(py, self.df.write_csv(path))?;
        Ok(())
    }

    /// Write a `DataFrame` to a Parquet file.
    fn write_parquet(&self, path: &str, py: Python) -> PyResult<()> {
        wait_for_future(py, self.df.write_parquet(path, None))?;
        Ok(())
    }

    /// Executes a query and writes the results to a partitioned JSON file.
    fn write_json(&self, path: &str, py: Python) -> PyResult<()> {
        wait_for_future(py, self.df.write_json(path))?;
        Ok(())
    }
}
