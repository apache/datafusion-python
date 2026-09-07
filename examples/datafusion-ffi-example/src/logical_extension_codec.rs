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

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arrow::datatypes::SchemaRef;
use datafusion::catalog::MemTable;
use datafusion::common::{DataFusionError, Result, TableReference};
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan, ScalarUDF};
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_proto::logical_plan::{DefaultLogicalExtensionCodec, LogicalExtensionCodec};
use datafusion_python_util::{ffi_task_context_provider_from_pycapsule, get_tokio_runtime};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::required_udf::{TaskContextProbe, resolve_required_udf};

const TABLE_PROVIDER_TOKEN: &[u8] = b"DFPYEXTP";
static NEXT_TABLE_PROVIDER_ID: AtomicU64 = AtomicU64::new(1);
static TABLE_PROVIDERS: OnceLock<Mutex<HashMap<u64, Arc<dyn TableProvider>>>> = OnceLock::new();

/// Hands a provider to another library in this process by token.
///
/// Encoding inserts, decoding removes. Two consequences worth knowing before
/// copying this:
///
/// - **Decode consumes the token.** Decoding the same encoded bytes twice
///   fails the second time with `Unknown ... table provider token`. That is
///   fine here because every plan is encoded immediately before the single
///   decode that consumes it, but it rules out anything that replays a stored
///   plan, retries a decode, or fans one encoded plan out to several readers.
/// - **An encode that is never decoded leaks.** Nothing expires entries, so a
///   plan that fails to reach its decoder keeps its provider alive for the
///   life of the process.
///
/// Both are acceptable for an example whose job is to show that Rust type
/// identity survives a trip through two other libraries. Neither is acceptable
/// in a real codec, which should encode metadata sufficient to rebuild the
/// provider rather than parking the object here.
fn table_providers() -> &'static Mutex<HashMap<u64, Arc<dyn TableProvider>>> {
    TABLE_PROVIDERS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn token_id(buf: &[u8], prefix: &[u8]) -> Option<u64> {
    let id: [u8; 8] = buf.strip_prefix(prefix)?.try_into().ok()?;
    Some(u64::from_le_bytes(id))
}

#[derive(Debug, Default)]
pub(crate) struct CallCounters {
    pub encode_udf: AtomicUsize,
    pub decode_udf: AtomicUsize,
    pub encode_table_provider: AtomicUsize,
    pub decode_table_provider: AtomicUsize,
    pub task_ctx: TaskContextProbe,
}

/// Example codec for objects owned by this extension library.
///
/// The table-provider token registry is intentionally process-local. It is a compact
/// example of preserving Rust type identity across three loaded libraries, not a
/// network serialization format. Production libraries should encode reconstructible
/// provider metadata rather than retaining objects in a global registry.
///
/// See [`table_providers`] for the token lifecycle, which is narrower than it
/// looks: a decode consumes its token, so the same encoded plan cannot be
/// decoded twice.
struct CountingLogicalExtensionCodec {
    inner: DefaultLogicalExtensionCodec,
    counters: Arc<CallCounters>,
    /// Scalar function every table-provider decode must resolve from the
    /// `TaskContext` it is handed. See [`crate::required_udf`].
    required_udf: Option<String>,
    /// Byte prefix identifying providers this codec owns. Distinct tokens let a
    /// test install several instances and observe which one the chain picks.
    token: Arc<[u8]>,
}

impl fmt::Debug for CountingLogicalExtensionCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CountingLogicalExtensionCodec")
            .field("inner", &self.inner)
            .field("counters", &self.counters)
            .finish_non_exhaustive()
    }
}

impl LogicalExtensionCodec for CountingLogicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        resolve_required_udf(self.required_udf.as_deref(), ctx, &self.counters.task_ctx)?;
        if let Some(id) = token_id(buf, &self.token) {
            self.counters
                .decode_table_provider
                .fetch_add(1, Ordering::SeqCst);
            return table_providers()
                .lock()
                .map_err(|err| DataFusionError::Internal(err.to_string()))?
                .remove(&id)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Unknown datafusion-ffi-example table provider token {id}"
                    ))
                });
        }
        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if node.downcast_ref::<MemTable>().is_some() {
            self.counters
                .encode_table_provider
                .fetch_add(1, Ordering::SeqCst);
            let id = NEXT_TABLE_PROVIDER_ID.fetch_add(1, Ordering::SeqCst);
            table_providers()
                .lock()
                .map_err(|err| DataFusionError::Internal(err.to_string()))?
                .insert(id, node);
            buf.extend_from_slice(&self.token);
            buf.extend_from_slice(&id.to_le_bytes());
            return Ok(());
        }
        self.inner.try_encode_table_provider(table_ref, node, buf)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.counters.decode_udf.fetch_add(1, Ordering::SeqCst);
        self.inner.try_decode_udf(name, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.counters.encode_udf.fetch_add(1, Ordering::SeqCst);
        self.inner.try_encode_udf(node, buf)
    }
}

#[pyclass(
    from_py_object,
    name = "MyLogicalExtensionCodec",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Clone)]
pub(crate) struct MyLogicalExtensionCodec {
    counters: Arc<CallCounters>,
    required_udf: Option<String>,
    token: Arc<[u8]>,
}

#[pymethods]
impl MyLogicalExtensionCodec {
    /// Build the codec.
    ///
    /// `require_udf_on_decode` names a scalar function that every table
    /// provider decode must find in the `TaskContext` it is handed. Leave it
    /// unset for the ordinary behaviour; set it to observe *which* session's
    /// registry the FFI decode callback actually receives.
    ///
    /// `provider_prefix` overrides [`TABLE_PROVIDER_TOKEN`], the byte prefix
    /// stamped on encoded table providers. Two instances built with different
    /// prefixes each own a disjoint slice of the wire format, which is what
    /// lets a test install both and tell from the decoded bytes which one the
    /// session's codec chain consulted.
    #[new]
    #[pyo3(signature = (require_udf_on_decode=None, provider_prefix=None))]
    fn new(require_udf_on_decode: Option<String>, provider_prefix: Option<&str>) -> Self {
        Self {
            counters: Arc::new(CallCounters::default()),
            required_udf: require_udf_on_decode,
            token: provider_prefix.map_or_else(
                || Arc::from(TABLE_PROVIDER_TOKEN),
                |prefix| Arc::from(prefix.as_bytes()),
            ),
        }
    }

    /// Number of decode calls that resolved `require_udf_on_decode`.
    fn task_context_udf_resolutions(&self) -> usize {
        self.counters.task_ctx.resolutions()
    }

    /// Session id of the `TaskContext` the most recent decode callback ran
    /// against, or `None` before any decode.
    fn last_task_context_session_id(&self) -> Option<String> {
        self.counters.task_ctx.last_session_id()
    }

    fn encode_udf_calls(&self) -> usize {
        self.counters.encode_udf.load(Ordering::SeqCst)
    }

    fn decode_udf_calls(&self) -> usize {
        self.counters.decode_udf.load(Ordering::SeqCst)
    }

    fn table_provider_encode_calls(&self) -> usize {
        self.counters.encode_table_provider.load(Ordering::SeqCst)
    }

    fn table_provider_decode_calls(&self) -> usize {
        self.counters.decode_table_provider.load(Ordering::SeqCst)
    }

    /// Export the codec, bound to the session it is being installed on.
    ///
    /// `session` supplies the `TaskContextProvider` the FFI decode callbacks
    /// resolve, so this library never constructs a `SessionContext` and the
    /// callbacks see the registry of the session running the query.
    fn __datafusion_logical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let inner: Arc<dyn LogicalExtensionCodec> = Arc::new(CountingLogicalExtensionCodec {
            inner: DefaultLogicalExtensionCodec {},
            counters: Arc::clone(&self.counters),
            required_udf: self.required_udf.clone(),
            token: Arc::clone(&self.token),
        });

        let runtime = get_tokio_runtime().handle().clone();
        let ctx_provider = ffi_task_context_provider_from_pycapsule(&session)?;
        let ffi = FFI_LogicalExtensionCodec::new(inner, Some(runtime), ctx_provider);

        PyCapsule::new_with_value(py, ffi, cr"datafusion_logical_extension_codec")
    }
}
