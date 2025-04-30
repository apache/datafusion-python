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

use crate::errors::{PyDataFusionError, PyDataFusionResult};
use crate::TokioRuntime;
use datafusion::common::ScalarValue;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Volatility;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyCapsule;
use pyo3::{prelude::*, BoundObject};
use std::future::Future;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

/// Utility to get the Tokio Runtime from Python
#[inline]
pub(crate) fn get_tokio_runtime() -> &'static TokioRuntime {
    // NOTE: Other pyo3 python libraries have had issues with using tokio
    // behind a forking app-server like `gunicorn`
    // If we run into that problem, in the future we can look to `delta-rs`
    // which adds a check in that disallows calls from a forked process
    // https://github.com/delta-io/delta-rs/blob/87010461cfe01563d91a4b9cd6fa468e2ad5f283/python/src/utils.rs#L10-L31
    static RUNTIME: OnceLock<TokioRuntime> = OnceLock::new();
    RUNTIME.get_or_init(|| TokioRuntime(tokio::runtime::Runtime::new().unwrap()))
}

/// Utility to get the Global Datafussion CTX
#[inline]
pub(crate) fn get_global_ctx() -> &'static SessionContext {
    static CTX: OnceLock<SessionContext> = OnceLock::new();
    CTX.get_or_init(SessionContext::new)
}

/// Utility to collect rust futures with GIL released
pub fn wait_for_future<F>(py: Python, f: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    let runtime: &Runtime = &get_tokio_runtime().0;
    py.allow_threads(|| runtime.block_on(f))
}

pub(crate) fn parse_volatility(value: &str) -> PyDataFusionResult<Volatility> {
    Ok(match value {
        "immutable" => Volatility::Immutable,
        "stable" => Volatility::Stable,
        "volatile" => Volatility::Volatile,
        value => {
            return Err(PyDataFusionError::Common(format!(
                "Unsupportad volatility type: `{value}`, supported \
                 values are: immutable, stable and volatile."
            )))
        }
    })
}

pub(crate) fn validate_pycapsule(capsule: &Bound<PyCapsule>, name: &str) -> PyResult<()> {
    let capsule_name = capsule.name()?;
    if capsule_name.is_none() {
        return Err(PyValueError::new_err(
            "Expected schema PyCapsule to have name set.",
        ));
    }

    let capsule_name = capsule_name.unwrap().to_str()?;
    if capsule_name != name {
        return Err(PyValueError::new_err(format!(
            "Expected name '{}' in PyCapsule, instead got '{}'",
            name, capsule_name
        )));
    }

    Ok(())
}
/// Convert a python object to a ScalarValue
pub(crate) fn py_obj_to_scalar_value(py: Python, obj: PyObject) -> ScalarValue {
    // Try extracting primitive types first
    if let Some(scalar) = try_extract_primitive(py, &obj) {
        return scalar;
    }

    // Try extracting datetime types
    if let Some(scalar) = try_extract_datetime(py, &obj) {
        return scalar;
    }

    // Try extracting date type
    if let Some(scalar) = try_extract_date(py, &obj) {
        return scalar;
    }

    // If we reach here, the type is unsupported
    panic!("Unsupported value type")
}

/// Try to extract primitive types (bool, numbers, string)
fn try_extract_primitive(py: Python, obj: &PyObject) -> Option<ScalarValue> {
    if let Ok(value) = obj.extract::<bool>(py) {
        Some(ScalarValue::Boolean(Some(value)))
    } else if let Ok(value) = obj.extract::<i64>(py) {
        Some(ScalarValue::Int64(Some(value)))
    } else if let Ok(value) = obj.extract::<u64>(py) {
        Some(ScalarValue::UInt64(Some(value)))
    } else if let Ok(value) = obj.extract::<f64>(py) {
        Some(ScalarValue::Float64(Some(value)))
    } else if let Ok(value) = obj.extract::<String>(py) {
        Some(ScalarValue::Utf8(Some(value)))
    } else {
        None
    }
}

/// Try to extract datetime object to TimestampNanosecond
fn try_extract_datetime(py: Python, obj: &PyObject) -> Option<ScalarValue> {
    let datetime_module = py.import("datetime").ok()?;
    let datetime_class = datetime_module.getattr("datetime").ok()?;

    if obj.is_instance(py, datetime_class).ok()? {
        // Extract timestamp as nanoseconds
        let timestamp = obj.call_method0(py, "timestamp").ok()?;
        let seconds_f64 = timestamp.extract::<f64>(py).ok()?;

        // Convert seconds to nanoseconds
        let nanos = (seconds_f64 * 1_000_000_000.0) as i64;
        return Some(ScalarValue::TimestampNanosecond(Some(nanos), None));
    }

    None
}

/// Try to extract date object to Date64
fn try_extract_date(py: Python, obj: &PyObject) -> Option<ScalarValue> {
    let datetime_module = py.import("datetime").ok()?;
    let date_class = datetime_module.getattr("date").ok()?;

    let any = PyAny::from(obj);
    let py_any: Bound<_, PyAny> = Bound::new(py, any).ok()?;
    if any.is_instance_of(py, date_class).ok()? {
        // Check if it's actually a datetime (which also is an instance of date)
        let datetime_class = datetime_module.getattr("datetime").ok()?;
        if any.is_instance(py, datetime_class).ok()? {
            return None; // Let the datetime handler take care of it
        }

        // Extract date components
        let year = obj.getattr(py, "year").ok()?.extract::<i32>(py).ok()?;
        let month = obj.getattr(py, "month").ok()?.extract::<u8>(py).ok()?;
        let day = obj.getattr(py, "day").ok()?.extract::<u8>(py).ok()?;

        // Calculate milliseconds since epoch (1970-01-01)
        let millis = date_to_millis(year, month as u32, day as u32)?;
        return Some(ScalarValue::Date64(Some(millis)));
    }

    None
}

/// Convert year, month, day to milliseconds since Unix epoch
fn date_to_millis(year: i32, month: u32, day: u32) -> Option<i64> {
    // Validate inputs
    if month < 1 || month > 12 || day < 1 || day > 31 {
        return None;
    }

    // Days in each month (non-leap year)
    let days_in_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    // Check if the day is valid for the given month
    let max_day = if month == 2 && is_leap_year(year) {
        29
    } else {
        days_in_month[month as usize]
    };

    if day > max_day {
        return None;
    }

    // Calculate days since epoch
    let mut total_days: i64 = 0;

    // Handle years
    let year_diff = year - 1970;
    if year_diff >= 0 {
        // Years from 1970 to year-1
        for y in 1970..year {
            total_days += if is_leap_year(y) { 366 } else { 365 };
        }
    } else {
        // Years from year to 1969
        for y in year..1970 {
            total_days -= if is_leap_year(y) { 366 } else { 365 };
        }
    }

    // Add days for the months in the current year
    for m in 1..month {
        total_days += if m == 2 && is_leap_year(year) {
            29
        } else {
            days_in_month[m as usize]
        } as i64;
    }

    // Add days in the current month
    total_days += (day as i64) - 1;

    // Convert days to milliseconds (86,400,000 ms per day)
    Some(total_days * 86_400_000)
}

/// Check if a year is a leap year
fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}
