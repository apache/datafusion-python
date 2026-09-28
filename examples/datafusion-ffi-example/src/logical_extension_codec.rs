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

use std::fmt;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
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

/// Default byte prefix stamped on every table provider this codec encodes.
const TABLE_PROVIDER_PREFIX: &[u8] = b"DFPYEXTP";

/// Format tag that follows the prefix. Bump it if the layout below changes.
const MEM_TABLE_FORMAT: &[u8] = b"MEMTBL1";

/// Write a [`MemTable`] as durable metadata: its schema and every batch of
/// every partition, so that a decoder anywhere can rebuild an equivalent
/// table from the bytes alone.
///
/// Layout, after the caller's provider prefix:
///
/// ```text
/// b"MEMTBL1" | u32 LE n_partitions | { u32 LE ipc_len | ipc stream }*
/// ```
///
/// Each partition is one Arrow IPC stream. The stream carries the schema, so
/// the decoder never has to trust a schema handed to it out of band.
fn encode_mem_table(table: &MemTable, buf: &mut Vec<u8>) -> Result<()> {
    let schema = table.schema();
    buf.extend_from_slice(MEM_TABLE_FORMAT);
    buf.extend_from_slice(&length_prefix(table.batches.len())?);

    for partition in &table.batches {
        // `MemTable` guards each partition with a tokio `RwLock`. This encode
        // runs on a tokio worker thread, where `blocking_read` panics, so
        // take the lock only if it is free. A partition that is mid-insert
        // is reported rather than waited for.
        let batches = partition.try_read().map_err(|_| {
            DataFusionError::Internal(
                "datafusion-ffi-example cannot encode a MemTable while a partition is locked"
                    .to_string(),
            )
        })?;

        let mut ipc = Vec::new();
        let mut writer = StreamWriter::try_new(&mut ipc, schema.as_ref())?;
        for batch in batches.iter() {
            writer.write(batch)?;
        }
        writer.finish()?;
        drop(writer);

        buf.extend_from_slice(&length_prefix(ipc.len())?);
        buf.extend_from_slice(&ipc);
    }
    Ok(())
}

/// Rebuild a [`MemTable`] from bytes written by [`encode_mem_table`].
///
/// The table's schema is the one carried inside the IPC streams, not the
/// `schema` argument DataFusion passes to `try_decode_table_provider`. A
/// payload that does not describe itself consistently is rejected here
/// instead of producing a table whose batches disagree with its schema.
fn decode_mem_table(payload: &[u8]) -> Result<MemTable> {
    let mut rest = payload.strip_prefix(MEM_TABLE_FORMAT).ok_or_else(|| {
        DataFusionError::Internal(
            "datafusion-ffi-example table provider payload has an unknown format tag".to_string(),
        )
    })?;

    let n_partitions = read_length_prefix(&mut rest)?;
    let mut schema: Option<SchemaRef> = None;
    let mut partitions: Vec<Vec<RecordBatch>> = Vec::with_capacity(n_partitions);

    for _ in 0..n_partitions {
        let ipc_len = read_length_prefix(&mut rest)?;
        if rest.len() < ipc_len {
            return Err(DataFusionError::Internal(
                "datafusion-ffi-example table provider payload is truncated".to_string(),
            ));
        }
        let (ipc, tail) = rest.split_at(ipc_len);
        rest = tail;

        let reader = StreamReader::try_new(Cursor::new(ipc), None)?;
        let ipc_schema = reader.schema();
        match &schema {
            None => schema = Some(ipc_schema),
            Some(first) if *first != ipc_schema => {
                return Err(DataFusionError::Internal(
                    "datafusion-ffi-example table provider partitions disagree on schema"
                        .to_string(),
                ));
            }
            Some(_) => {}
        }
        partitions.push(reader.collect::<std::result::Result<Vec<_>, _>>()?);
    }

    if !rest.is_empty() {
        return Err(DataFusionError::Internal(
            "datafusion-ffi-example table provider payload has trailing bytes".to_string(),
        ));
    }

    let schema = schema.ok_or_else(|| {
        DataFusionError::Internal(
            "datafusion-ffi-example table provider payload has no partitions".to_string(),
        )
    })?;
    MemTable::try_new(schema, partitions)
}

fn length_prefix(len: usize) -> Result<[u8; 4]> {
    u32::try_from(len)
        .map(u32::to_le_bytes)
        .map_err(|_| DataFusionError::Internal(format!("length {len} does not fit in u32")))
}

fn read_length_prefix(rest: &mut &[u8]) -> Result<usize> {
    let (head, tail) = rest.split_at_checked(4).ok_or_else(|| {
        DataFusionError::Internal(
            "datafusion-ffi-example table provider payload is truncated".to_string(),
        )
    })?;
    *rest = tail;
    let bytes: [u8; 4] = head.try_into().expect("split_at_checked returned 4 bytes");
    Ok(u32::from_le_bytes(bytes) as usize)
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
/// Table providers are encoded as durable metadata, see [`encode_mem_table`].
/// Nothing is retained between encode and decode, so the same bytes decode
/// any number of times, in any process, and an encoded plan that never
/// reaches a decoder costs nothing.
struct CountingLogicalExtensionCodec {
    inner: DefaultLogicalExtensionCodec,
    counters: Arc<CallCounters>,
    /// Scalar function every table-provider decode must resolve from the
    /// `TaskContext` it is handed. See [`crate::required_udf`].
    required_udf: Option<String>,
    /// Byte prefix identifying providers this codec owns. Distinct prefixes
    /// let a test install several instances and observe which one the chain
    /// picks.
    provider_prefix: Arc<[u8]>,
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
        if let Some(payload) = buf.strip_prefix(self.provider_prefix.as_ref()) {
            self.counters
                .decode_table_provider
                .fetch_add(1, Ordering::SeqCst);
            return Ok(Arc::new(decode_mem_table(payload)?));
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
        if let Some(table) = node.downcast_ref::<MemTable>() {
            self.counters
                .encode_table_provider
                .fetch_add(1, Ordering::SeqCst);
            buf.extend_from_slice(&self.provider_prefix);
            return encode_mem_table(table, buf);
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
    provider_prefix: Arc<[u8]>,
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
    /// `provider_prefix` overrides [`TABLE_PROVIDER_PREFIX`], the byte prefix
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
            provider_prefix: provider_prefix.map_or_else(
                || Arc::from(TABLE_PROVIDER_PREFIX),
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
            provider_prefix: Arc::clone(&self.provider_prefix),
        });

        let runtime = get_tokio_runtime().handle().clone();
        let ctx_provider = ffi_task_context_provider_from_pycapsule(&session)?;
        let ffi = FFI_LogicalExtensionCodec::new(inner, Some(runtime), ctx_provider);

        PyCapsule::new_with_value(py, ffi, cr"datafusion_logical_extension_codec")
    }
}
