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

use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::{FromPyArrow, PyArrowType, ToPyArrow};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::create_udf;
use datafusion::logical_expr::function::ScalarFunctionImplementation;
use datafusion::logical_expr::ScalarUDF;

use crate::expr::PyExpr;
use crate::utils::parse_volatility;

/// Create a DataFusion's UDF implementation from a python function
/// that expects pyarrow arrays. This is more efficient as it performs
/// a zero-copy of the contents.
fn to_rust_function(func: PyObject) -> ScalarFunctionImplementation {
    #[allow(deprecated)]
    datafusion::physical_plan::functions::make_scalar_function(
        move |args: &[ArrayRef]| -> Result<ArrayRef, DataFusionError> {
            Python::with_gil(|py| {
                // 1. cast args to Pyarrow arrays
                let py_args = args
                    .iter()
                    .map(|arg| arg.into_data().to_pyarrow(py).unwrap())
                    .collect::<Vec<_>>();
                let py_args = PyTuple::new(py, py_args);

                // 2. call function
                let value = func
                    .as_ref(py)
                    .call(py_args, None)
                    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

                // 3. cast to arrow::array::Array
                let array_data = ArrayData::from_pyarrow(value).unwrap();
                Ok(make_array(array_data))
            })
        },
    )
}

/// Represents a PyScalarUDF
#[pyclass(name = "ScalarUDF", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyScalarUDF {
    pub(crate) function: ScalarUDF,
}

#[pymethods]
impl PyScalarUDF {
    #[new]
    #[pyo3(signature=(name, func, input_types, return_type, volatility))]
    fn new(
        name: &str,
        func: PyObject,
        input_types: PyArrowType<Vec<DataType>>,
        return_type: PyArrowType<DataType>,
        volatility: &str,
    ) -> PyResult<Self> {
        let function = create_udf(
            name,
            input_types.0,
            Arc::new(return_type.0),
            parse_volatility(volatility)?,
            to_rust_function(func),
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
        Ok(format!("ScalarUDF({})", self.function.name()))
    }
}
