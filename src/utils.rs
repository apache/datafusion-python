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

use crate::common::data_type::PyScalarValue;
use crate::errors::{PyDataFusionError, PyDataFusionResult};
use crate::TokioRuntime;
use datafusion::common::ScalarValue;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Volatility;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::time::timeout;

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

/// Utility to get a Tokio Runtime with time explicitly enabled
#[inline]
pub(crate) fn get_tokio_runtime_with_time() -> &'static TokioRuntime {
    static RUNTIME_WITH_TIME: OnceLock<TokioRuntime> = OnceLock::new();
    RUNTIME_WITH_TIME.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        TokioRuntime(runtime)
    })
}

/// Utility to get the Global Datafussion CTX
#[inline]
pub(crate) fn get_global_ctx() -> &'static SessionContext {
    static CTX: OnceLock<SessionContext> = OnceLock::new();
    CTX.get_or_init(SessionContext::new)
}

/// Gets the Tokio runtime with time enabled and enters it, returning both the runtime and enter guard
/// This helps ensure that we don't forget to call enter() after getting the runtime
#[inline]
pub(crate) fn get_and_enter_tokio_runtime(
) -> (&'static Runtime, tokio::runtime::EnterGuard<'static>) {
    let runtime = &get_tokio_runtime_with_time().0;
    let enter_guard = runtime.enter();
    (runtime, enter_guard)
}

/// Utility to collect rust futures with GIL released and interrupt support
pub fn wait_for_future<F>(py: Python, f: F) -> PyResult<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (runtime, _enter_guard) = get_and_enter_tokio_runtime();

    // Spawn the task so we can poll it with timeouts
    let mut handle = runtime.spawn(f);

    // Release the GIL and poll the future with periodic signal checks
    py.allow_threads(|| {
        loop {
            // Poll the future with a timeout to allow periodic signal checking
            match runtime.block_on(timeout(Duration::from_millis(100), &mut handle)) {
                Ok(join_result) => {
                    // The inner task has completed before timeout
                    return join_result.map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Task failed: {}",
                            e
                        ))
                    });
                }
                Err(_elapsed) => {
                    // 100 ms elapsed without task completion → check Python signals
                    if let Err(py_exc) = Python::with_gil(|py| py.check_signals()) {
                        return Err(py_exc);
                    }
                    // Loop again, reintroducing another 100 ms timeout slice
                }
            }
        }
    })
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

pub(crate) fn py_obj_to_scalar_value(py: Python, obj: PyObject) -> PyResult<ScalarValue> {
    // convert Python object to PyScalarValue to ScalarValue

    let pa = py.import("pyarrow")?;

    // Convert Python object to PyArrow scalar
    let scalar = pa.call_method1("scalar", (obj,))?;

    // Convert PyArrow scalar to PyScalarValue
    let py_scalar = PyScalarValue::extract_bound(scalar.as_ref())
        .map_err(|e| PyValueError::new_err(format!("Failed to extract PyScalarValue: {}", e)))?;

    // Convert PyScalarValue to ScalarValue
    Ok(py_scalar.into())
}
