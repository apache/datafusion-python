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

use crate::errors::PyDataFusionError;
use datafusion::common::ScalarValue;
use pyo3::{prelude::*, IntoPyObjectExt};
use std::collections::BTreeMap;

#[pyclass(name = "Literal", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyLiteral {
    pub value: ScalarValue,
    pub metadata: Option<BTreeMap<String, String>>,
}

impl PyLiteral {
    pub fn new_with_metadata(
        value: ScalarValue,
        metadata: Option<BTreeMap<String, String>>,
    ) -> PyLiteral {
        Self { value, metadata }
    }
}

impl From<PyLiteral> for ScalarValue {
    fn from(lit: PyLiteral) -> ScalarValue {
        lit.value
    }
}

impl From<ScalarValue> for PyLiteral {
    fn from(value: ScalarValue) -> PyLiteral {
        PyLiteral {
            value,
            metadata: None,
        }
    }
}

macro_rules! extract_scalar_value {
    ($self: expr, $variant: ident) => {
        match &$self.value {
            ScalarValue::$variant(value) => Ok(*value),
            other => Err(unexpected_literal_value(other)),
        }
    };
}

#[pymethods]
impl PyLiteral {
    /// Get the data type of this literal value
    fn data_type(&self) -> String {
        format!("{}", self.value.data_type())
    }

    pub fn value_f32(&self) -> PyResult<Option<f32>> {
        extract_scalar_value!(self, Float32)
    }

    pub fn value_f64(&self) -> PyResult<Option<f64>> {
        extract_scalar_value!(self, Float64)
    }

    pub fn value_decimal128(&mut self) -> PyResult<(Option<i128>, u8, i8)> {
        match &self.value {
            ScalarValue::Decimal128(value, precision, scale) => Ok((*value, *precision, *scale)),
            other => Err(unexpected_literal_value(other)),
        }
    }

    pub fn value_i8(&self) -> PyResult<Option<i8>> {
        extract_scalar_value!(self, Int8)
    }

    pub fn value_i16(&self) -> PyResult<Option<i16>> {
        extract_scalar_value!(self, Int16)
    }

    pub fn value_i32(&self) -> PyResult<Option<i32>> {
        extract_scalar_value!(self, Int32)
    }

    pub fn value_i64(&self) -> PyResult<Option<i64>> {
        extract_scalar_value!(self, Int64)
    }

    pub fn value_u8(&self) -> PyResult<Option<u8>> {
        extract_scalar_value!(self, UInt8)
    }

    pub fn value_u16(&self) -> PyResult<Option<u16>> {
        extract_scalar_value!(self, UInt16)
    }

    pub fn value_u32(&self) -> PyResult<Option<u32>> {
        extract_scalar_value!(self, UInt32)
    }

    pub fn value_u64(&self) -> PyResult<Option<u64>> {
        extract_scalar_value!(self, UInt64)
    }

    pub fn value_date32(&self) -> PyResult<Option<i32>> {
        extract_scalar_value!(self, Date32)
    }

    pub fn value_date64(&self) -> PyResult<Option<i64>> {
        extract_scalar_value!(self, Date64)
    }

    pub fn value_time64(&self) -> PyResult<Option<i64>> {
        extract_scalar_value!(self, Time64Nanosecond)
    }

    pub fn value_timestamp(&mut self) -> PyResult<(Option<i64>, Option<String>)> {
        match &self.value {
            ScalarValue::TimestampNanosecond(iv, tz)
            | ScalarValue::TimestampMicrosecond(iv, tz)
            | ScalarValue::TimestampMillisecond(iv, tz)
            | ScalarValue::TimestampSecond(iv, tz) => {
                Ok((*iv, tz.as_ref().map(|s| s.as_ref().to_string())))
            }
            other => Err(unexpected_literal_value(other)),
        }
    }

    pub fn value_bool(&self) -> PyResult<Option<bool>> {
        extract_scalar_value!(self, Boolean)
    }

    pub fn value_string(&self) -> PyResult<Option<String>> {
        match &self.value {
            ScalarValue::Utf8(value) => Ok(value.clone()),
            other => Err(unexpected_literal_value(other)),
        }
    }

    pub fn value_interval_day_time(&self) -> PyResult<Option<(i32, i32)>> {
        match &self.value {
            ScalarValue::IntervalDayTime(Some(iv)) => Ok(Some((iv.days, iv.milliseconds))),
            ScalarValue::IntervalDayTime(None) => Ok(None),
            other => Err(unexpected_literal_value(other)),
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn into_type<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.value))
    }
}

fn unexpected_literal_value(value: &ScalarValue) -> PyErr {
    PyDataFusionError::Common(format!("getValue<T>() - Unexpected value: {value}")).into()
}
