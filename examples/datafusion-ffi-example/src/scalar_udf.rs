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

use arrow_array::{Array, BooleanArray};
use arrow_schema::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_ffi::udf::FFI_ScalarUDF;
use pyo3::types::PyCapsule;
use pyo3::{pyclass, pymethods, Bound, PyResult, Python};
use std::any::Any;
use std::sync::Arc;

#[pyclass(name = "IsNullUDF", module = "datafusion_ffi_example", subclass)]
#[derive(Debug, Clone)]
pub(crate) struct IsNullUDF {
    signature: Signature,
}

#[pymethods]
impl IsNullUDF {
    #[new]
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }

    fn __datafusion_scalar_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_scalar_udf".into();

        let func = Arc::new(ScalarUDF::from(self.clone()));
        let provider = FFI_ScalarUDF::from(func);

        PyCapsule::new(py, provider, Some(name))
    }
}

impl ScalarUDFImpl for IsNullUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "my_custom_is_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let input = &args.args[0];

        Ok(match input {
            ColumnarValue::Array(arr) => match arr.is_nullable() {
                true => {
                    let nulls = arr.nulls().unwrap();
                    let nulls = BooleanArray::from_iter(nulls.iter().map(|x| Some(!x)));
                    ColumnarValue::Array(Arc::new(nulls))
                }
                false => ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))),
            },
            ColumnarValue::Scalar(sv) => {
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(sv == &ScalarValue::Null)))
            }
        })
    }
}
