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

use std::any::Any;

use pyo3::{prelude::*, types::PyTuple};

use datafusion::arrow::array::{make_array, Array, ArrayData};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::FromPyArrow;
use datafusion::arrow::pyarrow::{PyArrowType, ToPyArrow};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Volatility};
use datafusion::logical_expr::{ScalarUDF, Signature};
use std::fmt::Debug;

use crate::expr::PyExpr;
use crate::utils::parse_volatility;

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
        let function = PythonUDF::new(
            name,
            input_types.0,
            return_type.0,
            parse_volatility(volatility)?,
            func,
        );

        Ok(Self {
            function: function.into(),
        })
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

/// Implements [`ScalarUDFImpl`] for functions that have a single signature and
/// return type.
pub struct PythonUDF {
    pub name: String,
    pub signature: Signature,
    // input types preserved as its a bit messy to get them from signature
    pub input_types: Vec<DataType>,
    pub return_type: DataType,
    pub func: PyObject,
}

impl Debug for PythonUDF {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("PythonUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("input_types", &self.input_types)
            .field("return_type", &self.return_type)
            .field("func", &"<FUNC>")
            .finish()
    }
}

impl PythonUDF {
    /// Create a new `PythonUDF` from a name, input types, return type and
    /// implementation.
    pub fn new(
        name: impl Into<String>,
        input_types: Vec<DataType>,
        return_type: DataType,
        volatility: Volatility,
        func: PyObject,
    ) -> Self {
        Self::new_with_signature(
            name,
            Signature::exact(input_types.clone(), volatility),
            input_types,
            return_type,
            func,
        )
    }

    /// Create a new `SimpleScalarUDF` from a name, signature, return type and
    /// implementation.
    pub fn new_with_signature(
        name: impl Into<String>,
        signature: Signature,
        input_types: Vec<DataType>,
        return_type: DataType,

        func: PyObject,
    ) -> Self {
        Self {
            name: name.into(),
            signature,
            input_types,
            return_type,
            func,
        }
    }
}

impl ScalarUDFImpl for PythonUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        let array_refs = ColumnarValue::values_to_arrays(args)?;
        let array_data: Result<_> = Python::with_gil(|py| {
            // 1. cast args to PyArrow arrays
            let py_args = array_refs
                .iter()
                .map(|arg| {
                    arg.into_data()
                        .to_pyarrow(py)
                        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let py_args = PyTuple::new_bound(py, py_args);

            // 2. call function
            let value = self
                .func
                .call_bound(py, py_args, None)
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

            // 3. cast to arrow::array::Array
            ArrayData::from_pyarrow_bound(value.bind(py))
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
        });

        Ok(make_array(array_data?).into())
    }
}
