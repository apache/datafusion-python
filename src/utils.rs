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
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Volatility;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
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
