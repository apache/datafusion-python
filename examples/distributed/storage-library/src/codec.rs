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

//! A physical codec that writes durable metadata.
//!
//! The other example crates in this repository park the live object in a
//! process-global `HashMap` and encode an integer token into it. That makes
//! Rust type identity observable in a test, and it is explicitly not a
//! pattern: the token means the same bytes cannot be decoded twice, one plan
//! cannot fan out to several readers, and a plan that never reaches a decoder
//! leaks. None of that is acceptable for a plan that leaves the process.
//!
//! This codec writes down what a fresh [`PartitionedParquetExec`] can be built
//! from -- the file paths and sizes, the projection, the row limit, and the
//! schema -- so decoding needs nothing from the encoding process. Sending the
//! same bytes to ten workers works, and so does sending them tomorrow.
//!
//! # Wire format
//!
//! ```text
//! DFXSTOR1 | json_len: u32 (LE) | json | arrow ipc schema
//! ```
//!
//! The magic is checked before anything else is read, and the trailing `1` is
//! a version this codec refuses to guess at. JSON carries the small scalar
//! fields because a human debugging a worker can read it; the schema is Arrow
//! IPC because that is the only encoding guaranteed to round-trip every Arrow
//! type, including extension types and field metadata.
//!
//! # Which error to raise
//!
//! Two classes appear below, and the split is DataFusion's own rule rather
//! than a preference. `DataFusionError::Internal` is documented as "due to
//! bugs in DataFusion", it appends *"please help us to resolve this by filing
//! a bug report"* to every message, and "a user should not be able to trigger
//! internal errors under normal circumstances by feeding in malformed
//! queries, bad data, etc."
//!
//! A codec reads bytes from somewhere else, so almost everything that can go
//! wrong here is bad data: a truncated payload, a version skew, a projection
//! index that is not a number. Those are `Execution`, because the person
//! reading the message needs to look at the payload, not at DataFusion's
//! issue tracker. `Internal` is left for the two things that really would be
//! this library's fault -- failing to encode an object it holds in memory,
//! and being handed a plan node that violates its own contract.

use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use datafusion::catalog::TableProvider;
use datafusion::common::{
    Result, TableReference, exec_datafusion_err, exec_err, internal_datafusion_err,
};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::{DefaultLogicalExtensionCodec, LogicalExtensionCodec};
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};

use crate::exec::{FileSlice, PartitionedParquetExec};
use crate::table_provider::PartitionedParquetTable;

/// Framing magic. The trailing digit is the payload version.
const MAGIC: &[u8; 8] = b"DFXSTOR1";

/// How often this codec claimed one of its own nodes.
///
/// Exposed to Python so a test can assert that *this* codec carried the node,
/// rather than inferring it from a query that merely succeeded. Both codecs
/// being installed does not mean yours saw the node -- see
/// `extension_codec_order`.
#[derive(Default, Debug)]
pub(crate) struct CodecCounters {
    pub(crate) encoded: AtomicUsize,
    pub(crate) decoded: AtomicUsize,
    pub(crate) declined: AtomicUsize,
    pub(crate) provider_encoded: AtomicUsize,
    pub(crate) provider_decoded: AtomicUsize,
}

pub(crate) struct DfxStoragePhysicalCodec {
    /// Anything this library does not own is handed to the default codec,
    /// whose error is the chain's "not mine" signal.
    inner: DefaultPhysicalExtensionCodec,
    pub(crate) counters: Arc<CodecCounters>,
}

impl DfxStoragePhysicalCodec {
    pub(crate) fn new(counters: Arc<CodecCounters>) -> Self {
        Self {
            inner: DefaultPhysicalExtensionCodec {},
            counters,
        }
    }
}

impl fmt::Debug for DfxStoragePhysicalCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DfxStoragePhysicalCodec")
            .finish_non_exhaustive()
    }
}

/// `Internal` on purpose: the schema being written is one this process is
/// already holding, so a failure here is this library's bug and not bad input.
fn schema_to_ipc_bytes(schema: &Schema) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)
            .map_err(|err| internal_datafusion_err!("dfx_storage: writing schema: {err}"))?;
        writer
            .finish()
            .map_err(|err| internal_datafusion_err!("dfx_storage: writing schema: {err}"))?;
    }
    Ok(buf)
}

fn schema_from_ipc_bytes(bytes: &[u8]) -> Result<Schema> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|err| exec_datafusion_err!("dfx_storage: reading schema: {err}"))?;
    Ok(reader.schema().as_ref().clone())
}

impl PhysicalExtensionCodec for DfxStoragePhysicalCodec {
    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        // Downcast to our own concrete type. Claiming a broad category --
        // `ForeignExecutionPlan`, say -- would take nodes from every library
        // installed after this one, and the query would still succeed, so
        // nothing would point at the codec that stole them.
        let Some(exec) = node.downcast_ref::<PartitionedParquetExec>() else {
            self.counters.declined.fetch_add(1, Ordering::SeqCst);
            return self.inner.try_encode(node, buf, proto_converter);
        };

        let descriptor = serde_json::json!({
            "files": exec.files.iter().map(|file| {
                serde_json::json!({ "path": file.path, "size": file.size })
            }).collect::<Vec<_>>(),
            "projection": exec.projection,
            "limit": exec.limit,
        });
        // `Internal`, like `schema_to_ipc_bytes`: serializing a value built
        // two lines up cannot fail on anything but a bug here.
        let json = serde_json::to_vec(&descriptor)
            .map_err(|err| internal_datafusion_err!("dfx_storage: encoding descriptor: {err}"))?;
        let schema = schema_to_ipc_bytes(&exec.table_schema)?;

        buf.extend_from_slice(MAGIC);
        let json_len = u32::try_from(json.len())
            .map_err(|_| exec_datafusion_err!("dfx_storage: descriptor too large to encode"))?;
        buf.extend_from_slice(&json_len.to_le_bytes());
        buf.extend_from_slice(&json);
        buf.extend_from_slice(&schema);

        self.counters.encoded.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // The chain routes a framed payload by id, so reaching this codec
        // already means the payload is ours. Checking the magic anyway is
        // cheap and turns a version skew into a clear error instead of a
        // misparse.
        let Some(rest) = buf.strip_prefix(MAGIC) else {
            self.counters.declined.fetch_add(1, Ordering::SeqCst);
            return self.inner.try_decode(buf, inputs, ctx, proto_converter);
        };
        if !inputs.is_empty() {
            return exec_err!(
                "PartitionedParquetExec is a leaf, got {} input(s)",
                inputs.len()
            );
        }

        let (len_bytes, rest) = rest.split_at_checked(4).ok_or_else(|| {
            exec_datafusion_err!("dfx_storage: payload truncated before descriptor length")
        })?;
        let json_len = u32::from_le_bytes(
            len_bytes
                .try_into()
                .map_err(|_| exec_datafusion_err!("dfx_storage: bad descriptor length"))?,
        ) as usize;
        let (json, schema_bytes) = rest.split_at_checked(json_len).ok_or_else(|| {
            exec_datafusion_err!(
                "dfx_storage: descriptor claims {json_len} bytes, {} remain",
                rest.len()
            )
        })?;

        let descriptor: serde_json::Value = serde_json::from_slice(json)
            .map_err(|err| exec_datafusion_err!("dfx_storage: bad descriptor: {err}"))?;
        let files = descriptor["files"]
            .as_array()
            .ok_or_else(|| exec_datafusion_err!("dfx_storage: descriptor has no file list"))?
            .iter()
            .map(|file| {
                let path = file["path"]
                    .as_str()
                    .ok_or_else(|| exec_datafusion_err!("dfx_storage: file entry has no path"))?;
                let size = file["size"].as_u64().ok_or_else(|| {
                    exec_datafusion_err!("dfx_storage: file entry {path} has no size")
                })?;
                Ok(FileSlice {
                    path: path.to_string(),
                    size,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        // Every element has to parse. `filter_map` here would drop the ones
        // that did not and hand back a *shorter* projection, which is not a
        // degraded answer but a different query: the indices are positional,
        // so losing one silently reads the wrong columns, and losing all of
        // them reads none. A codec is the last place that can tell a
        // malformed payload from a valid one, because everything downstream
        // sees a well-formed plan.
        let projection = match &descriptor["projection"] {
            // Absent and null both mean "every column".
            serde_json::Value::Null => None,
            serde_json::Value::Array(indices) => Some(
                indices
                    .iter()
                    .map(|index| {
                        index
                            .as_u64()
                            .and_then(|index| usize::try_from(index).ok())
                            .ok_or_else(|| {
                                exec_datafusion_err!(
                                    "dfx_storage: projection index {index} is not a column number"
                                )
                            })
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
            other => {
                return exec_err!(
                    "dfx_storage: projection must be a list of column numbers or null, got {other}"
                );
            }
        };
        let limit = match &descriptor["limit"] {
            serde_json::Value::Null => None,
            value => Some(
                value
                    .as_u64()
                    .and_then(|limit| usize::try_from(limit).ok())
                    .ok_or_else(|| {
                        exec_datafusion_err!("dfx_storage: limit {value} is not a row count")
                    })?,
            ),
        };
        let schema = Arc::new(schema_from_ipc_bytes(schema_bytes)?);

        self.counters.decoded.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(PartitionedParquetExec::new(
            files, schema, projection, limit,
        )?))
    }
}

/// Framing magic for the logical payload; the digit is its version.
const LOGICAL_MAGIC: &[u8; 8] = b"DFXSTOL1";

/// Carries this library's *table provider*, which a query planner forces.
///
/// A provider library might reasonably think a physical codec is enough --
/// its scan node is a physical node, after all. It is not. Installing any FFI
/// query planner means the session hands that planner the **logical** plan as
/// protobuf, and a logical plan holds its tables as `Arc<dyn TableProvider>`.
/// Encoding one is `try_encode_table_provider`, and the default codec has no
/// implementation, so without this codec a session that has *both* this
/// provider and any engine installed fails at `execution_plan()` with
/// "Error serializing custom table".
///
/// The payload is the directory, because everything else this provider holds
/// -- the file list, their sizes, the schema -- is read back from the
/// directory when it is rebuilt. Durable metadata again, for the same reason:
/// the process that decodes this has never seen the table registered.
pub(crate) struct DfxStorageLogicalCodec {
    inner: DefaultLogicalExtensionCodec,
    pub(crate) counters: Arc<CodecCounters>,
}

impl DfxStorageLogicalCodec {
    pub(crate) fn new(counters: Arc<CodecCounters>) -> Self {
        Self {
            inner: DefaultLogicalExtensionCodec {},
            counters,
        }
    }
}

impl fmt::Debug for DfxStorageLogicalCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DfxStorageLogicalCodec")
            .finish_non_exhaustive()
    }
}

impl LogicalExtensionCodec for DfxStorageLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<datafusion::logical_expr::Extension> {
        // This library defines no logical extension node.
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let Some(table) = node.downcast_ref::<PartitionedParquetTable>() else {
            self.counters.declined.fetch_add(1, Ordering::SeqCst);
            return self.inner.try_encode_table_provider(table_ref, node, buf);
        };
        buf.extend_from_slice(LOGICAL_MAGIC);
        buf.extend_from_slice(table.directory.as_bytes());
        self.counters
            .provider_encoded
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: arrow::datatypes::SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let Some(directory) = buf.strip_prefix(LOGICAL_MAGIC) else {
            self.counters.declined.fetch_add(1, Ordering::SeqCst);
            return self
                .inner
                .try_decode_table_provider(buf, table_ref, schema, ctx);
        };
        let directory = std::str::from_utf8(directory)
            .map_err(|err| exec_datafusion_err!("dfx_storage: bad directory in payload: {err}"))?;
        self.counters
            .provider_decoded
            .fetch_add(1, Ordering::SeqCst);
        // `schema` is the table schema recorded in the plan, and it is the one
        // the plan's projection indices were resolved against -- so it is the
        // schema this provider has to report, not one re-read from a file that
        // may have changed since. See `try_new_with_schema`.
        Ok(Arc::new(PartitionedParquetTable::try_new_with_schema(
            Path::new(directory),
            schema,
        )?))
    }
}
