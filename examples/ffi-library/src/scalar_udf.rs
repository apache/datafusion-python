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

use std::{ffi::CString, sync::Arc};

use arrow::array::BooleanArray;
use arrow_array::ArrayRef;
use datafusion::common::cast::as_int64_array;
use datafusion::logical_expr::create_udf;
use datafusion::logical_expr::Volatility;
use datafusion::physical_plan::ColumnarValue;
use datafusion::{arrow::datatypes::DataType, error::Result};
use datafusion_ffi::udf::FFI_ScalarUDF;
use pyo3::{prelude::*, types::PyCapsule};

#[pyclass(name = "IsEvenFunction", module = "datafusion_ffi_library", subclass)]
#[derive(Clone)]
pub struct IsEvenFunction {}

fn is_even(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    assert_eq!(args.len(), 1);
    let args = ColumnarValue::values_to_arrays(args)?;

    let values = as_int64_array(&args[0]).expect("cast failed");

    let array = values
        .iter()
        .map(|value| value.and_then(|v| if v == 0 { None } else { Some(v % 2 == 0) }))
        .collect::<BooleanArray>();

    Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
}

#[pymethods]
impl IsEvenFunction {
    #[new]
    fn new() -> Self {
        Self {}
    }

    fn __datafusion_scalar_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let name = CString::new("datafusion_scalar_udf").unwrap();

        let func = create_udf(
            "is_even",
            vec![DataType::Int64],
            DataType::Boolean,
            Volatility::Immutable,
            Arc::new(is_even),
        );

        let ffi_func: FFI_ScalarUDF = (Arc::new(func)).try_into()?;

        PyCapsule::new(py, ffi_func, Some(name))
    }
}
