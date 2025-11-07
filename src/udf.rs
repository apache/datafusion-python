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
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{Field, FieldRef};
use arrow::pyarrow::ToPyArrow;
use datafusion::arrow::array::{make_array, ArrayData};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::{FromPyArrow, PyArrowType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_ffi::udf::FFI_ScalarUDF;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};

use crate::array::PyArrowArrayExportable;
use crate::errors::{py_datafusion_err, to_datafusion_err, PyDataFusionResult};
use crate::expr::PyExpr;
use crate::utils::{parse_volatility, validate_pycapsule};

/// This struct holds the Python written function that is a
/// ScalarUDF.
#[derive(Debug)]
struct PythonFunctionScalarUDF {
    name: String,
    func: Py<PyAny>,
    signature: Signature,
    return_field: FieldRef,
}

impl PythonFunctionScalarUDF {
    fn new(
        name: String,
        func: Py<PyAny>,
        input_fields: Vec<Field>,
        return_field: Field,
        volatility: Volatility,
    ) -> Self {
        let input_types = input_fields.iter().map(|f| f.data_type().clone()).collect();
        let signature = Signature::exact(input_types, volatility);
        Self {
            name,
            func,
            signature,
            return_field: Arc::new(return_field),
        }
    }
}

impl Eq for PythonFunctionScalarUDF {}
impl PartialEq for PythonFunctionScalarUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.signature == other.signature
            && self.return_field == other.return_field
            && Python::attach(|py| self.func.bind(py).eq(other.func.bind(py)).unwrap_or(false))
    }
}

impl Hash for PythonFunctionScalarUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
        self.return_field.hash(state);

        Python::attach(|py| {
            let py_hash = self.func.bind(py).hash().unwrap_or(0); // Handle unhashable objects

            state.write_isize(py_hash);
        });
    }
}

impl ScalarUDFImpl for PythonFunctionScalarUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        unimplemented!()
    }

    fn return_field_from_args(
        &self,
        _args: ReturnFieldArgs,
    ) -> datafusion::common::Result<FieldRef> {
        Ok(Arc::clone(&self.return_field))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let num_rows = args.number_rows;
        Python::attach(|py| {
            // 1. cast args to Pyarrow arrays
            let py_args = args
                .args
                .into_iter()
                .zip(args.arg_fields)
                .map(|(arg, field)| {
                    let array = arg.to_array(num_rows)?;
                    PyArrowArrayExportable::new(array, field)
                        .to_pyarrow(py)
                        .map_err(to_datafusion_err)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let py_args = PyTuple::new(py, py_args).map_err(to_datafusion_err)?;

            // 2. call function
            let value = self
                .func
                .call(py, py_args, None)
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

            // 3. cast to arrow::array::Array
            let array_data = ArrayData::from_pyarrow_bound(value.bind(py))
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            Ok(ColumnarValue::Array(make_array(array_data)))
        })
    }
}

/// Represents a PyScalarUDF
#[pyclass(frozen, name = "ScalarUDF", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyScalarUDF {
    pub(crate) function: ScalarUDF,
}

#[pymethods]
impl PyScalarUDF {
    #[new]
    #[pyo3(signature=(name, func, input_types, return_type, volatility))]
    fn new(
        name: String,
        func: Py<PyAny>,
        input_types: PyArrowType<Vec<Field>>,
        return_type: PyArrowType<Field>,
        volatility: &str,
    ) -> PyResult<Self> {
        let py_function = PythonFunctionScalarUDF::new(
            name,
            func,
            input_types.0,
            return_type.0,
            parse_volatility(volatility)?,
        );
        let function = ScalarUDF::new_from_impl(py_function);

        Ok(Self { function })
    }

    #[staticmethod]
    pub fn from_pycapsule(func: Bound<'_, PyAny>) -> PyDataFusionResult<Self> {
        if func.hasattr("__datafusion_scalar_udf__")? {
            let capsule = func.getattr("__datafusion_scalar_udf__")?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>().map_err(py_datafusion_err)?;
            validate_pycapsule(capsule, "datafusion_scalar_udf")?;

            let udf = unsafe { capsule.reference::<FFI_ScalarUDF>() };
            let udf: Arc<dyn ScalarUDFImpl> = udf.into();

            Ok(Self {
                function: ScalarUDF::new_from_shared_impl(udf),
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
