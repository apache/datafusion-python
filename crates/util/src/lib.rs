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

use std::future::Future;
use std::ptr::NonNull;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::execution::context::{QueryPlanner, SessionContext};
use datafusion::logical_expr::Volatility;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::physical_optimizer::FFI_PhysicalOptimizerRule;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use datafusion_ffi::query_planner::FFI_QueryPlanner;
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use pyo3::exceptions::{PyImportError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyType};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio::time::sleep;

pub mod errors;
pub use crate::errors::to_datafusion_err;
use crate::errors::{PyDataFusionError, PyDataFusionResult};

/// Utility to get the Tokio Runtime from Python
#[inline]
pub fn get_tokio_runtime() -> &'static Runtime {
    // NOTE: Other pyo3 python libraries have had issues with using tokio
    // behind a forking app-server like `gunicorn`
    // If we run into that problem, in the future we can look to `delta-rs`
    // which adds a check in that disallows calls from a forked process
    // https://github.com/delta-io/delta-rs/blob/87010461cfe01563d91a4b9cd6fa468e2ad5f283/python/src/utils.rs#L10-L31
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

#[inline]
pub fn is_ipython_env(py: Python) -> &'static bool {
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
pub fn get_global_ctx() -> &'static Arc<SessionContext> {
    static CTX: OnceLock<Arc<SessionContext>> = OnceLock::new();
    CTX.get_or_init(|| Arc::new(SessionContext::new()))
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
    let runtime: &Runtime = get_tokio_runtime();
    const INTERVAL_CHECK_SIGNALS: Duration = Duration::from_millis(1_000);

    // Some fast running processes that generate many `wait_for_future` calls like
    // PartitionedDataFrameStreamReader::next require checking for interrupts early
    py.run(cr"pass", None, None)?;
    py.check_signals()?;

    py.detach(|| {
        runtime.block_on(async {
            tokio::pin!(fut);
            loop {
                tokio::select! {
                    res = &mut fut => break Ok(res),
                    _ = sleep(INTERVAL_CHECK_SIGNALS) => {
                        Python::attach(|py| {
                                // Execute a no-op Python statement to trigger signal processing.
                                // This is necessary because py.check_signals() alone doesn't
                                // actually check for signals - it only raises an exception if
                                // a signal was already set during a previous Python API call.
                                // Running even trivial Python code forces the interpreter to
                                // process any pending signals (like KeyboardInterrupt).
                                py.run(cr"pass", None, None)?;
                                py.check_signals()
                        })?;
                    }
                }
            }
        })
    })
}

/// Spawn a [`Future`] on the Tokio runtime and wait for completion
/// while respecting Python signal handling.
pub fn spawn_future<F, T>(py: Python, fut: F) -> PyDataFusionResult<T>
where
    F: Future<Output = datafusion::common::Result<T>> + Send + 'static,
    T: Send + 'static,
{
    let rt = get_tokio_runtime();
    let handle: JoinHandle<datafusion::common::Result<T>> = rt.spawn(fut);
    // Wait for the join handle while respecting Python signal handling.
    // We handle errors in two steps so `?` maps the error types correctly:
    // 1) convert any Python-related error from `wait_for_future` into `PyDataFusionError`
    // 2) convert any DataFusion error (inner result) into `PyDataFusionError`
    let inner_result = wait_for_future(py, async {
        // handle.await yields `Result<datafusion::common::Result<T>, JoinError>`
        // map JoinError into a DataFusion error so the async block returns
        // `datafusion::common::Result<T>` (i.e. Result<T, DataFusionError>)
        match handle.await {
            Ok(inner) => inner,
            Err(join_err) => Err(to_datafusion_err(join_err)),
        }
    })?; // converts PyErr -> PyDataFusionError

    // `inner_result` is `datafusion::common::Result<T>`; use `?` to convert
    // the inner DataFusion error into `PyDataFusionError` via `From` and
    // return the inner `T` on success.
    Ok(inner_result?)
}

pub fn parse_volatility(value: &str) -> PyDataFusionResult<Volatility> {
    Ok(match value {
        "immutable" => Volatility::Immutable,
        "stable" => Volatility::Stable,
        "volatile" => Volatility::Volatile,
        value => {
            return Err(PyDataFusionError::Common(format!(
                "Unsupported volatility type: `{value}`, supported \
                 values are: immutable, stable and volatile."
            )));
        }
    })
}

pub fn validate_pycapsule(capsule: &Bound<PyCapsule>, name: &str) -> PyResult<()> {
    let capsule_name = capsule.name()?;
    if capsule_name.is_none() {
        return Err(PyValueError::new_err(format!(
            "Expected {name} PyCapsule to have name set."
        )));
    }

    let capsule_name = unsafe { capsule_name.unwrap().as_cstr().to_str()? };
    if capsule_name != name {
        return Err(PyValueError::new_err(format!(
            "Expected name '{name}' in PyCapsule, instead got '{capsule_name}'"
        )));
    }

    Ok(())
}

pub fn table_provider_from_pycapsule<'py>(
    mut obj: Bound<'py, PyAny>,
    session: Bound<'py, PyAny>,
) -> PyResult<Option<Arc<dyn TableProvider>>> {
    if obj.hasattr("__datafusion_table_provider__")? {
        obj = obj
            .getattr("__datafusion_table_provider__")?
            .call1((session,)).map_err(|err| {
            let py = obj.py();
            if err.get_type(py).is(PyType::new::<PyTypeError>(py)) {
                PyImportError::new_err("Incompatible libraries. DataFusion 52.0.0 introduced an incompatible signature change for table providers. Either downgrade DataFusion or upgrade your function library.")
            } else {
                err
            }
        })?;
    }

    if let Ok(capsule) = obj.cast::<PyCapsule>() {
        let data: NonNull<FFI_TableProvider> = capsule
            .pointer_checked(Some(c"datafusion_table_provider"))?
            .cast();
        let provider = unsafe { data.as_ref() };
        let provider: Arc<dyn TableProvider> = provider.into();

        Ok(Some(provider))
    } else {
        Ok(None)
    }
}

pub fn create_logical_extension_capsule<'py>(
    py: Python<'py>,
    codec: &FFI_LogicalExtensionCodec,
) -> PyResult<Bound<'py, PyCapsule>> {
    let name = cr"datafusion_logical_extension_codec".into();
    let codec = codec.clone();

    PyCapsule::new(py, codec, Some(name))
}

pub fn ffi_logical_codec_from_pycapsule(obj: Bound<PyAny>) -> PyResult<FFI_LogicalExtensionCodec> {
    let attr_name = "__datafusion_logical_extension_codec__";
    let capsule = if obj.hasattr(attr_name)? {
        obj.getattr(attr_name)?.call0()?
    } else {
        obj
    };

    let capsule = capsule.cast::<PyCapsule>()?;
    let data: NonNull<FFI_LogicalExtensionCodec> = capsule
        .pointer_checked(Some(c"datafusion_logical_extension_codec"))?
        .cast();
    let codec = unsafe { data.as_ref() };

    Ok(codec.clone())
}

pub fn create_physical_extension_capsule<'py>(
    py: Python<'py>,
    codec: &FFI_PhysicalExtensionCodec,
) -> PyResult<Bound<'py, PyCapsule>> {
    let name = cr"datafusion_physical_extension_codec".into();
    let codec = codec.clone();

    PyCapsule::new(py, codec, Some(name))
}

/// Define a `<fn_name>(obj) -> PyResult<Arc<$output_type>>` extractor that
/// accepts either a raw `PyCapsule` carrying `$ffi_type` or any object
/// exposing `__<capsule_name>__()` that returns one.
///
/// Use this when `Arc<$output_type>: From<&$ffi_type>` (infallible
/// conversion). For fallible conversions use [`try_from_pycapsule!`]
/// instead.
#[macro_export]
macro_rules! from_pycapsule {
    ($fn_name:ident, $capsule_name:literal, $ffi_type:ty, $output_type:ty) => {
        pub fn $fn_name(
            obj: &$crate::pyo3::Bound<$crate::pyo3::PyAny>,
        ) -> $crate::pyo3::PyResult<std::sync::Arc<$output_type>> {
            use $crate::pyo3::prelude::*;
            use $crate::pyo3::types::PyCapsule;

            let mut obj = obj.clone();
            if obj.hasattr(concat!("__", $capsule_name, "__"))? {
                obj = obj.getattr(concat!("__", $capsule_name, "__"))?.call0()?;
            }
            let capsule = obj.cast::<PyCapsule>().map_err(|_| {
                $crate::errors::py_datafusion_err(concat!(
                    "Invalid ",
                    $capsule_name,
                    ". Does not contain PyCapsule object."
                ))
            })?;
            $crate::validate_pycapsule(&capsule, $capsule_name)?;

            let expected_name = std::ffi::CString::new($capsule_name)
                .expect("capsule name must not contain interior NUL bytes");
            let data: std::ptr::NonNull<$ffi_type> = capsule
                .pointer_checked(Some(expected_name.as_c_str()))?
                .cast();
            let output_obj = unsafe { data.as_ref() };
            let output_obj: std::sync::Arc<$output_type> = output_obj.into();

            Ok(output_obj)
        }
    };
}

/// Same shape as [`from_pycapsule!`] but for FFI types whose conversion
/// into `Arc<$output_type>` is fallible (uses `TryFrom`).
#[macro_export]
macro_rules! try_from_pycapsule {
    ($fn_name:ident, $capsule_name:literal, $ffi_type:ty, $output_type:ty) => {
        pub fn $fn_name(
            obj: &$crate::pyo3::Bound<$crate::pyo3::PyAny>,
        ) -> $crate::pyo3::PyResult<std::sync::Arc<$output_type>> {
            use $crate::pyo3::prelude::*;
            use $crate::pyo3::types::PyCapsule;

            let mut obj = obj.clone();
            if obj.hasattr(concat!("__", $capsule_name, "__"))? {
                obj = obj.getattr(concat!("__", $capsule_name, "__"))?.call0()?;
            }
            let capsule = obj.cast::<PyCapsule>().map_err(|_| {
                $crate::errors::py_datafusion_err(concat!(
                    "Invalid ",
                    $capsule_name,
                    ". Does not contain PyCapsule object."
                ))
            })?;
            $crate::validate_pycapsule(&capsule, $capsule_name)?;

            let expected_name = std::ffi::CString::new($capsule_name)
                .expect("capsule name must not contain interior NUL bytes");
            let data: std::ptr::NonNull<$ffi_type> = capsule
                .pointer_checked(Some(expected_name.as_c_str()))?
                .cast();
            let output_obj = unsafe { data.as_ref() };
            let output_obj: std::sync::Arc<$output_type> = output_obj
                .try_into()
                .map_err($crate::errors::py_datafusion_err)?;

            Ok(output_obj)
        }
    };
}

// Re-export pyo3 so the macros expand inside downstream crates without
// requiring an explicit pyo3 dep at the call site.
#[doc(hidden)]
pub use pyo3;

from_pycapsule!(
    physical_codec_from_pycapsule,
    "datafusion_physical_extension_codec",
    FFI_PhysicalExtensionCodec,
    dyn PhysicalExtensionCodec
);

from_pycapsule!(
    physical_optimizer_rule_from_pycapsule,
    "datafusion_physical_optimizer_rule",
    FFI_PhysicalOptimizerRule,
    dyn PhysicalOptimizerRule + Send + Sync
);

from_pycapsule!(
    query_planner_from_pycapsule,
    "datafusion_query_planner_v1",
    FFI_QueryPlanner,
    dyn QueryPlanner + Send + Sync
);

try_from_pycapsule!(
    task_context_from_pycapsule,
    "datafusion_task_context_provider",
    FFI_TaskContextProvider,
    TaskContext
);
