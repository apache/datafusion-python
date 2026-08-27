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

//! Support for exercising the `TaskContext` a codec is handed when it decodes.
//!
//! A codec exported over FFI carries a `TaskContextProvider`, and the decode
//! callbacks in `datafusion-ffi` resolve it to a `TaskContext` before calling
//! into the codec. Nothing in the example codecs read anything out of that
//! context, so which session it belongs to was untestable: the token
//! registries they use are keyed by an integer and ignore the registry.
//!
//! The codecs can now be asked to resolve a named scalar function from the
//! context they are given on every decode, which makes the answer observable.
//! Because the codecs take their provider from the session they are installed
//! on, a function registered on the host with `register_udf` resolves.

use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::execution::TaskContext;
use datafusion_common::error::Result as DataFusionResult;
use datafusion_common::plan_err;

/// Resolves `required` against `ctx`, the context the decode callback was given.
///
/// `Ok(())` when nothing was requested. Otherwise the name must be present in
/// the context's scalar function registry, and `resolutions` counts each
/// success so a test can tell a resolved lookup from a skipped one.
pub(crate) fn resolve_required_udf(
    required: Option<&str>,
    ctx: &TaskContext,
    resolutions: &AtomicUsize,
) -> DataFusionResult<()> {
    let Some(name) = required else {
        return Ok(());
    };

    if ctx.scalar_functions().contains_key(name) {
        resolutions.fetch_add(1, Ordering::SeqCst);
        return Ok(());
    }

    // A fresh SessionContext still carries every built-in, so report the count
    // rather than the whole registry.
    plan_err!(
        "datafusion-ffi-example: decode could not resolve scalar function '{name}' \
         in the task context it was handed (session '{}', {} scalar functions registered)",
        ctx.session_id(),
        ctx.scalar_functions().len()
    )
}
