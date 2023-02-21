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

use crate::errors::py_runtime_err;
use datafusion_common::ScalarValue;
use pyo3::prelude::*;

#[pyclass(name = "Literal", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyLiteral {
    pub value: ScalarValue,
}

impl From<PyLiteral> for ScalarValue {
    fn from(lit: PyLiteral) -> ScalarValue {
        lit.value
    }
}

impl From<ScalarValue> for PyLiteral {
    fn from(value: ScalarValue) -> PyLiteral {
        PyLiteral { value }
    }
}

#[pymethods]
impl PyLiteral {
    /// Get the data type of this literal value
    fn data_type(&self) -> String {
        format!("{}", self.value.get_datatype())
    }

    fn value_i32(&self) -> PyResult<i32> {
        if let ScalarValue::Int32(Some(n)) = &self.value {
            Ok(*n)
        } else {
            Err(py_runtime_err("Cannot access value as i32"))
        }
    }

    fn value_i64(&self) -> PyResult<i64> {
        if let ScalarValue::Int64(Some(n)) = &self.value {
            Ok(*n)
        } else {
            Err(py_runtime_err("Cannot access value as i64"))
        }
    }

    fn value_str(&self) -> PyResult<String> {
        if let ScalarValue::Utf8(Some(str)) = &self.value {
            Ok(str.clone())
        } else {
            Err(py_runtime_err("Cannot access value as string"))
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.value))
    }
}
