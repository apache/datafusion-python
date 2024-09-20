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

use std::ops::Range;
use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayData, ArrayRef};
use datafusion::logical_expr::window_state::WindowAggState;
use datafusion::prelude::create_udwf;
use datafusion::scalar::ScalarValue;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::{FromPyArrow, PyArrowType, ToPyArrow};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{PartitionEvaluator, PartitionEvaluatorFactory, WindowUDF};
use pyo3::types::{PyList, PyTuple};

use crate::expr::PyExpr;
use crate::utils::parse_volatility;

#[derive(Debug)]
struct RustPartitionEvaluator {
    evaluator: PyObject,
}

impl RustPartitionEvaluator {
    fn new(evaluator: PyObject) -> Self {
        Self { evaluator }
    }
}

impl PartitionEvaluator for RustPartitionEvaluator {
    fn memoize(&mut self, _state: &mut WindowAggState) -> Result<()> {
        Python::with_gil(|py| self.evaluator.bind(py).call_method0("memoize").map(|_| ()))
            .map_err(|e| DataFusionError::Execution(format!("{e}")))
    }

    fn get_range(&self, idx: usize, n_rows: usize) -> Result<Range<usize>> {
        Python::with_gil(|py| {
            let py_args = vec![idx.to_object(py), n_rows.to_object(py)];
            let py_args = PyTuple::new_bound(py, py_args);

            self.evaluator
                .bind(py)
                .call_method1("get_range", py_args)
                .and_then(|v| {
                    let tuple: Bound<'_, PyTuple> = v.extract()?;
                    if tuple.len() != 2 {
                        return Err(PyValueError::new_err(format!(
                            "Expected get_range to return tuple of length 2. Received length {}",
                            tuple.len()
                        )));
                    }

                    let start: usize = tuple.get_item(0).unwrap().extract()?;
                    let end: usize = tuple.get_item(1).unwrap().extract()?;

                    Ok(Range { start, end })
                })
        })
        .map_err(|e| DataFusionError::Execution(format!("{e}")))
    }

    fn is_causal(&self) -> bool {
        Python::with_gil(|py| {
            self.evaluator
                .bind(py)
                .call_method0("is_causal")
                .and_then(|v| v.extract())
        })
        .unwrap_or(false)
    }

    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        Python::with_gil(|py| {
            // 1. cast args to Pyarrow array
            let mut py_args = values
                .iter()
                .map(|arg| arg.into_data().to_pyarrow(py).unwrap())
                .collect::<Vec<_>>();
            py_args.push(num_rows.to_object(py));
            let py_args = PyTuple::new_bound(py, py_args);

            // 2. call function
            self.evaluator
                .bind(py)
                .call_method1("evaluate_all", py_args)
                .map_err(|e| DataFusionError::Execution(format!("{e}")))
                .map(|v| {
                    let array_data = ArrayData::from_pyarrow_bound(&v).unwrap();
                    make_array(array_data)
                })
        })
    }

    fn evaluate(&mut self, values: &[ArrayRef], range: &Range<usize>) -> Result<ScalarValue> {
        Python::with_gil(|py| {
            // 1. cast args to Pyarrow array
            let mut py_args = values
                .iter()
                .map(|arg| arg.into_data().to_pyarrow(py).unwrap())
                .collect::<Vec<_>>();
            py_args.push(range.start.to_object(py));
            py_args.push(range.end.to_object(py));
            let py_args = PyTuple::new_bound(py, py_args);

            // 2. call function
            self.evaluator
                .bind(py)
                .call_method1("evaluate", py_args)
                .and_then(|v| v.extract())
                .map_err(|e| DataFusionError::Execution(format!("{e}")))
        })
    }

    fn evaluate_all_with_rank(
        &self,
        num_rows: usize,
        ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        Python::with_gil(|py| {
            let ranks = ranks_in_partition
                .iter()
                .map(|r| PyTuple::new_bound(py, vec![r.start, r.end]));

            // 1. cast args to Pyarrow array
            let py_args = vec![num_rows.to_object(py), PyList::new_bound(py, ranks).into()];

            let py_args = PyTuple::new_bound(py, py_args);

            // 2. call function
            self.evaluator
                .bind(py)
                .call_method1("evaluate_all_with_rank", py_args)
                .map_err(|e| DataFusionError::Execution(format!("{e}")))
                .map(|v| {
                    let array_data = ArrayData::from_pyarrow_bound(&v).unwrap();
                    make_array(array_data)
                })
        })
    }

    fn supports_bounded_execution(&self) -> bool {
        Python::with_gil(|py| {
            self.evaluator
                .bind(py)
                .call_method0("supports_bounded_execution")
                .and_then(|v| v.extract())
        })
        .unwrap_or(false)
    }

    fn uses_window_frame(&self) -> bool {
        Python::with_gil(|py| {
            self.evaluator
                .bind(py)
                .call_method0("uses_window_frame")
                .and_then(|v| v.extract())
        })
        .unwrap_or(false)
    }

    fn include_rank(&self) -> bool {
        Python::with_gil(|py| {
            self.evaluator
                .bind(py)
                .call_method0("include_rank")
                .and_then(|v| v.extract())
        })
        .unwrap_or(false)
    }
}

pub fn to_rust_partition_evaluator(evalutor: PyObject) -> PartitionEvaluatorFactory {
    Arc::new(move || -> Result<Box<dyn PartitionEvaluator>> {
        let evalutor = Python::with_gil(|py| {
            evalutor
                .call0(py)
                .map_err(|e| DataFusionError::Execution(format!("{e}")))
        })?;
        Ok(Box::new(RustPartitionEvaluator::new(evalutor)))
    })
}

/// Represents an WindowUDF
#[pyclass(name = "WindowUDF", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyWindowUDF {
    pub(crate) function: WindowUDF,
}

#[pymethods]
impl PyWindowUDF {
    #[new]
    #[pyo3(signature=(name, evaluator, input_type, return_type, volatility))]
    fn new(
        name: &str,
        evaluator: PyObject,
        input_type: PyArrowType<DataType>,
        return_type: PyArrowType<DataType>,
        volatility: &str,
    ) -> PyResult<Self> {
        let function = create_udwf(
            name,
            input_type.0,
            Arc::new(return_type.0),
            parse_volatility(volatility)?,
            to_rust_partition_evaluator(evaluator),
        );
        Ok(Self { function })
    }

    /// creates a new PyExpr with the call of the udf
    #[pyo3(signature = (*args))]
    fn __call__(&self, args: Vec<PyExpr>) -> PyResult<PyExpr> {
        let args = args.iter().map(|e| e.expr.clone()).collect();
        Ok(self.function.call(args).into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("WindowUDF({})", self.function.name()))
    }
}
