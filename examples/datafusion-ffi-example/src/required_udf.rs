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
//! A codec exported over FFI is built with a `TaskContextProvider`, and the
//! decode callbacks in `datafusion-ffi` resolve that provider to a
//! `TaskContext` before calling into the codec. Nothing in the example codecs
//! read anything out of that context, so which session it belongs to was
//! untestable: the token registries they use are keyed by an integer and
//! ignore the registry entirely.
//!
//! The codecs can now be asked to resolve a named scalar function from the
//! context they are given on every decode, which makes the answer observable.
//! Two names matter:
//!
//! - [`LIBRARY_LOCAL_UDF_NAME`] is registered by [`new_codec_context`] on the
//!   context this library owns and hands to the FFI codec, so a decode
//!   callback resolves it.
//! - A function registered only on the *host* `SessionContext` does not
//!   resolve, because the decode callback never sees the host's registry.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_schema::DataType;
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionContext;
use datafusion_common::error::Result as DataFusionResult;
use datafusion_common::plan_err;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

/// Name of the scalar function registered on the context this library owns.
pub(crate) const LIBRARY_LOCAL_UDF_NAME: &str = "library_local_marker";

/// Placeholder scalar function used only as a registry entry.
///
/// Decode callbacks look it up by name to prove which `TaskContext` they were
/// handed. It is never invoked, so the body is unreachable in practice.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LibraryLocalUDF {
    signature: Signature,
}

impl LibraryLocalUDF {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for LibraryLocalUDF {
    fn name(&self) -> &str {
        LIBRARY_LOCAL_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        plan_err!("{LIBRARY_LOCAL_UDF_NAME} exists only as a registry entry and cannot be invoked")
    }
}

/// Builds the context a codec keeps for its own FFI `TaskContextProvider`.
///
/// This is the context every extension library has to conjure in order to
/// export a codec, and it is a plain empty session apart from the marker
/// function. The host's registrations are not in it, which is the point the
/// codec tests make observable.
pub(crate) fn new_codec_context() -> Arc<SessionContext> {
    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::from(LibraryLocalUDF::new()));
    Arc::new(ctx)
}

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
