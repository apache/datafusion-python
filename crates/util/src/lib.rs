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
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Volatility;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::physical_optimizer::FFI_PhysicalOptimizerRule;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use datafusion_ffi::query_planner::FFI_QueryPlanner;
use datafusion_ffi::table_provider::FFI_TableProvider;
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

    let capsule_name = unsafe { capsule_name.unwrap().as_cstr().to_str() }
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    if capsule_name != name {
        return Err(PyValueError::new_err(format!(
            "Expected name '{name}' in PyCapsule, instead got '{capsule_name}'"
        )));
    }

    Ok(())
}

/// Reject an FFI struct built against a different major version of
/// `datafusion-ffi`.
///
/// `found` comes from the struct's own `version` function pointer, which
/// reports the major version of the library that produced it.
///
/// This is a diagnostic, not a soundness guarantee. Reading `version` out of
/// the struct already assumes the local field layout, and `version` is not the
/// first field on any of these types, so a sufficiently different layout can
/// fault before this ever runs. What it buys is a clear error for the case that
/// actually happens -- an extension library compiled against a different
/// DataFusion -- rather than undefined behaviour on first use, which is what
/// `datafusion_ffi::version` exists for.
///
/// Not every FFI type carries a version. `FFI_TaskContextProvider`,
/// `FFI_TableProviderFactory`, and `FFI_ExtensionOptions` have no such field,
/// so their importers cannot check and are not expected to.
///
/// # If the FFI ABI stabilizes
///
/// Exact equality is the right test only while `datafusion_ffi::version`
/// tracks the DataFusion crate's semver major, which it does today
/// (`env!("CARGO_PKG_VERSION")`, `.major`). That number therefore moves on
/// every major release whether or not the ABI actually changed.
///
/// Should a version span become compatible, **this function body is the only
/// thing to change** -- callers pass a `found` value and no policy. Relaxing it
/// at a call site instead would reintroduce the split this helper exists to
/// remove.
///
/// The likelier fix is upstream, not here: if the ABI is stable but `version`
/// still follows the crate major, upstream's own compatibility marker is wrong
/// for every consumer, not just this one. Prefer waiting for
/// `datafusion_ffi::version` to reflect the real ABI over inventing a range
/// policy locally.
pub fn check_ffi_version(kind: &str, found: u64) -> PyResult<()> {
    let expected = datafusion_ffi::version();
    if found != expected {
        return Err(PyImportError::new_err(format!(
            "Incompatible DataFusion {kind} major version {found}; expected {expected}. \
             Rebuild the library providing this object against a matching DataFusion."
        )));
    }
    Ok(())
}

pub fn table_provider_from_pycapsule<'py>(
    mut obj: Bound<'py, PyAny>,
    session: Bound<'py, PyAny>,
) -> PyResult<Option<Arc<dyn TableProvider>>> {
    obj = call_capsule_getter(obj, "__datafusion_table_provider__", Some(&session))?;

    if let Ok(capsule) = obj.cast::<PyCapsule>() {
        let data: NonNull<FFI_TableProvider> = capsule
            .pointer_checked(Some(c"datafusion_table_provider"))?
            .cast();
        let provider = unsafe { data.as_ref() };
        check_ffi_version("table provider", unsafe { (provider.version)() })?;
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
    let codec = codec.clone();

    PyCapsule::new_with_value(py, codec, cr"datafusion_logical_extension_codec")
}

/// Calls `obj.__<attr_name>__(session)`, or `obj.__<attr_name>__()` when no
/// session is supplied.
///
/// The session is how an exporting library obtains the codecs and task context
/// of the session it is being installed on, instead of inventing one of its
/// own. `None` is for the reverse direction, where `obj` *is* a session and is
/// being asked for what it holds.
/// Every capsule getter must go through here rather than calling `getattr`
/// directly, so that the mapping from a refused argument to a useful error
/// lives in one place. Three importers previously each had their own copy and
/// each missed later corrections to it.
pub fn call_capsule_getter<'py>(
    obj: Bound<'py, PyAny>,
    attr_name: &str,
    session: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    if !obj.hasattr(attr_name)? {
        return Ok(obj);
    }

    let getter = obj.getattr(attr_name)?;
    let result = match session {
        Some(session) => getter.call1((session,)),
        None => getter.call0(),
    };

    result.map_err(|err| {
        let py = obj.py();
        if session.is_none() || !err.get_type(py).is(PyType::new::<PyTypeError>(py)) {
            return err;
        }

        // Not every `TypeError` here means the getter refused the argument. One
        // raised *inside* a correctly-signed getter would otherwise be reported
        // as a version mismatch, sending an extension author to upgrade a
        // library that is already correct.
        //
        // The two are distinguishable: an arity mismatch is raised by the call
        // machinery before the getter's frame exists, so nothing unwinds and no
        // traceback is attached. An error from the body unwinds that frame and
        // carries one.
        if err.traceback(py).is_some() {
            return err;
        }

        let import_err = PyImportError::new_err(format!(
            "Incompatible libraries. `{attr_name}` must accept the SessionContext it \
             is being installed on. Upgrade the library providing this object."
        ));
        // Keep the original reachable as `__cause__` rather than discarding it.
        import_err.set_cause(py, Some(err));
        import_err
    })
}

pub fn ffi_logical_codec_from_pycapsule<'py>(
    obj: Bound<'py, PyAny>,
    session: Option<&Bound<'py, PyAny>>,
) -> PyResult<FFI_LogicalExtensionCodec> {
    let capsule = call_capsule_getter(obj, "__datafusion_logical_extension_codec__", session)?;

    let capsule = capsule.cast::<PyCapsule>()?;
    let data: NonNull<FFI_LogicalExtensionCodec> = capsule
        .pointer_checked(Some(c"datafusion_logical_extension_codec"))?
        .cast();
    let codec = unsafe { data.as_ref() };
    check_ffi_version("logical extension codec", unsafe { (codec.version)() })?;

    Ok(codec.clone())
}

pub fn ffi_physical_codec_from_pycapsule<'py>(
    obj: Bound<'py, PyAny>,
    session: Option<&Bound<'py, PyAny>>,
) -> PyResult<FFI_PhysicalExtensionCodec> {
    let capsule = call_capsule_getter(obj, "__datafusion_physical_extension_codec__", session)?;

    let capsule = capsule.cast::<PyCapsule>()?;
    validate_pycapsule(capsule, "datafusion_physical_extension_codec")?;
    let data: NonNull<FFI_PhysicalExtensionCodec> = capsule
        .pointer_checked(Some(c"datafusion_physical_extension_codec"))?
        .cast();
    let codec = unsafe { data.as_ref() };
    check_ffi_version("physical extension codec", unsafe { (codec.version)() })?;

    Ok(codec.clone())
}

/// Extracts the `FFI_TaskContextProvider` a session exposes.
///
/// An extension library exporting a codec needs one for the decode callbacks
/// its codec will receive. Taking the host's means those callbacks resolve
/// names against the session that is actually running the query, and removes
/// any need for the library to construct a `SessionContext` of its own.
pub fn ffi_task_context_provider_from_pycapsule(
    session: &Bound<PyAny>,
) -> PyResult<FFI_TaskContextProvider> {
    let capsule = call_capsule_getter(
        session.clone(),
        "__datafusion_task_context_provider__",
        None,
    )?;

    let capsule = capsule.cast::<PyCapsule>()?;
    validate_pycapsule(capsule, "datafusion_task_context_provider")?;
    let data: NonNull<FFI_TaskContextProvider> = capsule
        .pointer_checked(Some(c"datafusion_task_context_provider"))?
        .cast();
    let provider = unsafe { data.as_ref() };

    Ok(provider.clone())
}

pub fn create_query_planner_capsule<'py>(
    py: Python<'py>,
    planner: &FFI_QueryPlanner,
) -> PyResult<Bound<'py, PyCapsule>> {
    PyCapsule::new_with_value(py, planner.clone(), cr"datafusion_query_planner")
}

pub fn ffi_query_planner_from_pycapsule<'py>(
    obj: &Bound<'py, PyAny>,
    session: Option<&Bound<'py, PyAny>>,
) -> PyResult<FFI_QueryPlanner> {
    let capsule = call_capsule_getter(obj.clone(), "__datafusion_query_planner__", session)?;

    let capsule = capsule.cast::<PyCapsule>()?;
    validate_pycapsule(capsule, "datafusion_query_planner")?;
    let data: NonNull<FFI_QueryPlanner> = capsule
        .pointer_checked(Some(c"datafusion_query_planner"))?
        .cast();
    let planner = unsafe { data.as_ref() };
    check_ffi_version("query planner", unsafe { (planner.version)() })?;

    Ok(planner.clone())
}

pub fn create_physical_extension_capsule<'py>(
    py: Python<'py>,
    codec: &FFI_PhysicalExtensionCodec,
) -> PyResult<Bound<'py, PyCapsule>> {
    let codec = codec.clone();

    PyCapsule::new_with_value(py, codec, cr"datafusion_physical_extension_codec")
}

/// Define a `<fn_name>(obj) -> PyResult<Arc<$output_type>>` extractor that
/// accepts either a raw `PyCapsule` carrying `$ffi_type` or any object
/// exposing `__<capsule_name>__()` that returns one.
///
/// Use this when `Arc<$output_type>: From<&$ffi_type>` (infallible
/// conversion). For fallible conversions use [`try_from_pycapsule!`]
/// instead.
///
/// The generated extractor does not check the FFI major version, because not
/// every FFI type carries one. If `$ffi_type` has a `version` field, call
/// [`check_ffi_version`] on it yourself, as the hand-written extractors in this
/// crate do.
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

// There is deliberately no `physical_codec_from_pycapsule` here. These macros
// call the getter with no arguments, which is right for the two hooks below but
// wrong for `__datafusion_physical_extension_codec__`, which takes the session
// it is being installed on. Use `ffi_physical_codec_from_pycapsule`, which
// passes the session, and convert with `(&ffi).into()` if you need an
// `Arc<dyn PhysicalExtensionCodec>`.
from_pycapsule!(
    physical_optimizer_rule_from_pycapsule,
    "datafusion_physical_optimizer_rule",
    FFI_PhysicalOptimizerRule,
    dyn PhysicalOptimizerRule + Send + Sync
);

try_from_pycapsule!(
    task_context_from_pycapsule,
    "datafusion_task_context_provider",
    FFI_TaskContextProvider,
    TaskContext
);
