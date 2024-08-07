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

use std::sync::Arc;

use pyo3::{prelude::*, types::PyTuple};

use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::{PyArrowType, ToPyArrow};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion_expr::{create_udaf, Accumulator, AccumulatorFactoryFunction, AggregateUDF};

use crate::expr::PyExpr;
use crate::utils::parse_volatility;

#[derive(Debug)]
struct RustAccumulator {
    accum: PyObject,
}

impl RustAccumulator {
    fn new(accum: PyObject) -> Self {
        Self { accum }
    }
}

impl Accumulator for RustAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Python::with_gil(|py| self.accum.bind(py).call_method0("state")?.extract())
            .map_err(|e| DataFusionError::Execution(format!("{e}")))
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Python::with_gil(|py| self.accum.bind(py).call_method0("evaluate")?.extract())
            .map_err(|e| DataFusionError::Execution(format!("{e}")))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        Python::with_gil(|py| {
            // 1. cast args to Pyarrow array
            let py_args = values
                .iter()
                .map(|arg| arg.into_data().to_pyarrow(py).unwrap())
                .collect::<Vec<_>>();
            let py_args = PyTuple::new_bound(py, py_args);

            // 2. call function
            self.accum
                .bind(py)
                .call_method1("update", py_args)
                .map_err(|e| DataFusionError::Execution(format!("{e}")))?;

            Ok(())
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        Python::with_gil(|py| {
            // // 1. cast states to Pyarrow arrays
            let py_states: Result<Vec<PyObject>> = states
                .iter()
                .map(|state| {
                    state
                        .into_data()
                        .to_pyarrow(py)
                        .map_err(|e| DataFusionError::Execution(format!("{e}")))
                })
                .collect();

            // 2. call merge
            self.accum
                .bind(py)
                .call_method1("merge", (py_states?,))
                .map_err(|e| DataFusionError::Execution(format!("{e}")))?;

            Ok(())
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        Python::with_gil(|py| {
            // 1. cast args to Pyarrow array
            let py_args = values
                .iter()
                .map(|arg| arg.into_data().to_pyarrow(py).unwrap())
                .collect::<Vec<_>>();
            let py_args = PyTuple::new_bound(py, py_args);

            // 2. call function
            self.accum
                .bind(py)
                .call_method1("retract_batch", py_args)
                .map_err(|e| DataFusionError::Execution(format!("{e}")))?;

            Ok(())
        })
    }

    fn supports_retract_batch(&self) -> bool {
        Python::with_gil(
            |py| match self.accum.bind(py).call_method0("supports_retract_batch") {
                Ok(x) => x.extract().unwrap_or(false),
                Err(_) => false,
            },
        )
    }
}

pub fn to_rust_accumulator(accum: PyObject) -> AccumulatorFactoryFunction {
    Arc::new(move |_| -> Result<Box<dyn Accumulator>> {
        let accum = Python::with_gil(|py| {
            accum
                .call0(py)
                .map_err(|e| DataFusionError::Execution(format!("{e}")))
        })?;
        Ok(Box::new(RustAccumulator::new(accum)))
    })
}

/// Represents an AggregateUDF
#[pyclass(name = "AggregateUDF", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyAggregateUDF {
    pub(crate) function: AggregateUDF,
}

#[pymethods]
impl PyAggregateUDF {
    #[new]
    #[pyo3(signature=(name, accumulator, input_type, return_type, state_type, volatility))]
    fn new(
        name: &str,
        accumulator: PyObject,
        input_type: PyArrowType<Vec<DataType>>,
        return_type: PyArrowType<DataType>,
        state_type: PyArrowType<Vec<DataType>>,
        volatility: &str,
    ) -> PyResult<Self> {
        let function = create_udaf(
            name,
            input_type.0,
            Arc::new(return_type.0),
            parse_volatility(volatility)?,
            to_rust_accumulator(accumulator),
            Arc::new(state_type.0),
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
        Ok(format!("AggregateUDF({})", self.function.name()))
    }
}
