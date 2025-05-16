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

use datafusion_ffi::udf::{FFI_ScalarUDF, ForeignScalarUDF};
use pyo3::types::PyCapsule;
use pyo3::{prelude::*, types::PyTuple};

use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::FromPyArrow;
use datafusion::arrow::pyarrow::{PyArrowType, ToPyArrow};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::ScalarFunctionImplementation;
use datafusion::logical_expr::ScalarUDF;
use datafusion::logical_expr::{create_udf, ColumnarValue};

use crate::errors::to_datafusion_err;
use crate::errors::{py_datafusion_err, PyDataFusionResult};
use crate::expr::PyExpr;
use crate::utils::{parse_volatility, validate_pycapsule};

/// Create a Rust callable function from a python function that expects pyarrow arrays
fn pyarrow_function_to_rust(
    func: PyObject,
) -> impl Fn(&[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
    move |args: &[ArrayRef]| -> Result<ArrayRef, DataFusionError> {
        Python::with_gil(|py| {
            // 1. cast args to Pyarrow arrays
            let py_args = args
                .iter()
                .map(|arg| {
                    arg.into_data()
                        .to_pyarrow(py)
                        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let py_args = PyTuple::new(py, py_args).map_err(to_datafusion_err)?;

            // 2. call function
            let value = func
                .call(py, py_args, None)
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

            // 3. cast to arrow::array::Array
            let array_data = ArrayData::from_pyarrow_bound(value.bind(py))
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            Ok(make_array(array_data))
        })
    }
}

/// Create a DataFusion's UDF implementation from a python function
/// that expects pyarrow arrays. This is more efficient as it performs
/// a zero-copy of the contents.
fn to_scalar_function_impl(func: PyObject) -> ScalarFunctionImplementation {
    // Make the python function callable from rust
    let pyarrow_func = pyarrow_function_to_rust(func);

    // Convert input/output from datafusion ColumnarValue to arrow arrays
    Arc::new(move |args: &[ColumnarValue]| {
        let array_refs = ColumnarValue::values_to_arrays(args)?;
        let array_result = pyarrow_func(&array_refs)?;
        Ok(array_result.into())
    })
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
            return_type.0,
            parse_volatility(volatility)?,
            to_scalar_function_impl(func),
        );
        Ok(Self { function })
    }

    #[staticmethod]
    pub fn from_pycapsule(func: Bound<'_, PyAny>) -> PyDataFusionResult<Self> {
        if func.hasattr("__datafusion_scalar_udf__")? {
            let capsule = func.getattr("__datafusion_scalar_udf__")?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>().map_err(py_datafusion_err)?;
            validate_pycapsule(capsule, "datafusion_scalar_udf")?;

            let udf = unsafe { capsule.reference::<FFI_ScalarUDF>() };
            let udf: ForeignScalarUDF = udf.try_into()?;

            Ok(Self {
                function: udf.into(),
            })
        } else {
            Err(crate::errors::PyDataFusionError::Common(
                "__datafusion_scalar_udf__ does not exist on ScalarUDF object.".to_string(),
            ))
        }
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
