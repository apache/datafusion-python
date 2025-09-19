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

use crate::{
    common::data_type::PyScalarValue,
    errors::{py_datafusion_err, PyDataFusionError, PyDataFusionResult},
    TokioRuntime,
};
use datafusion::{
    common::ScalarValue, datasource::TableProvider, execution::context::SessionContext,
    logical_expr::Volatility,
};
use datafusion_ffi::table_provider::{FFI_TableProvider, ForeignTableProvider};
use pyo3::prelude::*;
use pyo3::{exceptions::PyValueError, types::PyCapsule};
use std::ffi::CString;
use std::{
    ffi::CStr,
    future::Future,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::{runtime::Runtime, time::sleep};

pub const TABLE_PROVIDER_CAPSULE_NAME_STR: &str = "datafusion_table_provider";
/// Return a static CStr for the PyCapsule name.
///
/// We create this lazily from a `CString` to avoid unsafe const
/// initialization from a byte literal and to satisfy compiler lints.
pub fn table_provider_capsule_name() -> &'static CStr {
    static NAME: OnceLock<CString> = OnceLock::new();
    NAME.get_or_init(|| CString::new(TABLE_PROVIDER_CAPSULE_NAME_STR).unwrap())
        .as_c_str()
}

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

#[inline]
pub(crate) fn is_ipython_env(py: Python) -> &'static bool {
    static IS_IPYTHON_ENV: OnceLock<bool> = OnceLock::new();
    IS_IPYTHON_ENV.get_or_init(|| {
        py.import("IPython")
            .and_then(|ipython| ipython.call_method0("get_ipython"))
            .map(|ipython| !ipython.is_none())
            .unwrap_or(false)
    })
}

/// Utility to get the Global Datafussion CTX
#[inline]
pub(crate) fn get_global_ctx() -> &'static SessionContext {
    static CTX: OnceLock<SessionContext> = OnceLock::new();
    CTX.get_or_init(SessionContext::new)
}

/// Utility to collect rust futures with GIL released and respond to
/// Python interrupts such as ``KeyboardInterrupt``. If a signal is
/// received while the future is running, the future is aborted and the
/// corresponding Python exception is raised.
pub fn wait_for_future<F>(py: Python, fut: F) -> PyResult<F::Output>
where
    F: Future + Send,
    F::Output: Send,
{
    let runtime: &Runtime = &get_tokio_runtime().0;
    const INTERVAL_CHECK_SIGNALS: Duration = Duration::from_millis(1_000);

    py.allow_threads(|| {
        runtime.block_on(async {
            tokio::pin!(fut);
            loop {
                tokio::select! {
                    res = &mut fut => break Ok(res),
                    _ = sleep(INTERVAL_CHECK_SIGNALS) => {
                        Python::with_gil(|py| py.check_signals())?;
                    }
                }
            }
        })
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
            "Expected name '{name}' in PyCapsule, instead got '{capsule_name}'"
        )));
    }

    Ok(())
}

pub(crate) fn foreign_table_provider_from_capsule(
    capsule: &Bound<PyCapsule>,
) -> PyResult<ForeignTableProvider> {
    validate_pycapsule(capsule, TABLE_PROVIDER_CAPSULE_NAME_STR)?;
    Ok(unsafe { capsule.reference::<FFI_TableProvider>() }.into())
}

pub(crate) fn try_table_provider_from_object(
    provider: &Bound<'_, PyAny>,
) -> PyResult<Option<Arc<dyn TableProvider>>> {
    if !provider.hasattr("__datafusion_table_provider__")? {
        return Ok(None);
    }

    let capsule = provider.getattr("__datafusion_table_provider__")?.call0()?;
    let capsule = capsule.downcast::<PyCapsule>().map_err(py_datafusion_err)?;
    let provider = foreign_table_provider_from_capsule(capsule)?;
    let provider: Arc<dyn TableProvider> = Arc::new(provider);

    Ok(Some(provider))
}

pub(crate) fn py_obj_to_scalar_value(py: Python, obj: PyObject) -> PyResult<ScalarValue> {
    // convert Python object to PyScalarValue to ScalarValue

    let pa = py.import("pyarrow")?;

    // Convert Python object to PyArrow scalar
    let scalar = pa.call_method1("scalar", (obj,))?;

    // Convert PyArrow scalar to PyScalarValue
    let py_scalar = PyScalarValue::extract_bound(scalar.as_ref())
        .map_err(|e| PyValueError::new_err(format!("Failed to extract PyScalarValue: {e}")))?;

    // Convert PyScalarValue to ScalarValue
    Ok(py_scalar.into())
}
