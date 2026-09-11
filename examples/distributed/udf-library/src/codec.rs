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

//! Codecs that make this library's functions portable.
//!
//! A UDF library that stops at exporting functions is only usable in the
//! process that registered them. The moment a plan referencing
//! `dfx_net_revenue` is serialized and read somewhere else, *something* has to
//! turn that name back into a function, and there are exactly two candidates:
//! the receiving session's function registry, or a codec.
//!
//! Both codecs here are name-only: `try_encode_*` writes nothing, and
//! `try_decode_*` rebuilds from `name`. That shape is supported directly --
//! an encoder that writes no bytes leaves `fun_definition` unset, and the
//! decoder then tries the registry first and the codec second. So installing
//! this library's codec on a worker is an *alternative* to registering the
//! three functions there, not an addition to it. Either is enough; neither is
//! a failure that shows up before the query runs.
//!
//! There are two codecs because there are two plan layers and this library
//! cannot know which one its callers will serialize. A distributed engine
//! shipping physical plans exercises only the physical one; `LogicalPlan.
//! to_bytes` exercises only the logical one.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::common::{Result, not_impl_err};
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_proto::logical_plan::{DefaultLogicalExtensionCodec, LogicalExtensionCodec};
use datafusion_proto::physical_plan::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};

use crate::functions::{aggregate_by_name, scalar_by_name, window_by_name};

/// Counts, so a test can assert this codec did the work rather than infer it
/// from a query that merely succeeded.
#[derive(Default, Debug)]
pub(crate) struct CodecCounters {
    pub(crate) decoded: AtomicUsize,
    pub(crate) declined: AtomicUsize,
}

/// Reject a payload for a function whose name is its whole encoding.
///
/// Checking `name` before `buf` is the order that matters. An empty payload
/// carries no codec id, so it is the one path where a payload is offered to
/// every installed codec in turn -- meaning this hook can be called with
/// another library's function name. Trusting `buf` first would have this codec
/// answer for names it does not own.
fn reject_payload(name: &str, buf: &[u8]) -> Result<()> {
    if buf.is_empty() {
        return Ok(());
    }
    not_impl_err!(
        "{name} is encoded by name and carries no payload, but {} bytes were supplied",
        buf.len()
    )
}

macro_rules! decode_by_name {
    ($self:ident, $name:expr, $buf:expr, $lookup:ident, $kind:literal) => {{
        let Some(function) = $lookup($name) else {
            $self.counters.declined.fetch_add(1, Ordering::SeqCst);
            return not_impl_err!("{} is not a dfx_udfs {}", $name, $kind);
        };
        reject_payload($name, $buf)?;
        $self.counters.decoded.fetch_add(1, Ordering::SeqCst);
        Ok(function)
    }};
}

/// Logical half. See the module docs for why there are two.
pub(crate) struct DfxUdfsLogicalCodec {
    inner: DefaultLogicalExtensionCodec,
    pub(crate) counters: Arc<CodecCounters>,
}

impl DfxUdfsLogicalCodec {
    pub(crate) fn new(counters: Arc<CodecCounters>) -> Self {
        Self {
            inner: DefaultLogicalExtensionCodec {},
            counters,
        }
    }
}

impl fmt::Debug for DfxUdfsLogicalCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DfxUdfsLogicalCodec")
            .finish_non_exhaustive()
    }
}

impl LogicalExtensionCodec for DfxUdfsLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        ctx: &datafusion::execution::TaskContext,
    ) -> Result<datafusion::logical_expr::Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &datafusion::common::TableReference,
        schema: arrow_schema::SchemaRef,
        ctx: &datafusion::execution::TaskContext,
    ) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &datafusion::common::TableReference,
        node: Arc<dyn datafusion::datasource::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.inner.try_encode_table_provider(table_ref, node, buf)
    }

    /// Writes nothing: returning `Ok` with an empty buffer is how a codec
    /// says "encoded by name".
    fn try_encode_udf(&self, _node: &ScalarUDF, _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        decode_by_name!(self, name, buf, scalar_by_name, "scalar function")
    }

    fn try_encode_udaf(&self, _node: &AggregateUDF, _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        decode_by_name!(self, name, buf, aggregate_by_name, "aggregate function")
    }

    fn try_encode_udwf(&self, _node: &WindowUDF, _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        decode_by_name!(self, name, buf, window_by_name, "window function")
    }
}

/// Physical half. This is the one a distributed engine exercises, because it
/// ships physical plans.
pub(crate) struct DfxUdfsPhysicalCodec {
    inner: DefaultPhysicalExtensionCodec,
    pub(crate) counters: Arc<CodecCounters>,
}

impl DfxUdfsPhysicalCodec {
    pub(crate) fn new(counters: Arc<CodecCounters>) -> Self {
        Self {
            inner: DefaultPhysicalExtensionCodec {},
            counters,
        }
    }
}

impl fmt::Debug for DfxUdfsPhysicalCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DfxUdfsPhysicalCodec")
            .finish_non_exhaustive()
    }
}

impl PhysicalExtensionCodec for DfxUdfsPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn datafusion::physical_plan::ExecutionPlan>],
        ctx: &datafusion::execution::TaskContext,
        proto_converter: &dyn datafusion_proto::physical_plan::PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        // This library owns no execution plan nodes, only functions.
        self.inner.try_decode(buf, inputs, ctx, proto_converter)
    }

    fn try_encode(
        &self,
        node: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn datafusion_proto::physical_plan::PhysicalProtoConverterExtension,
    ) -> Result<()> {
        self.inner.try_encode(node, buf, proto_converter)
    }

    fn try_encode_udf(&self, _node: &ScalarUDF, _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        decode_by_name!(self, name, buf, scalar_by_name, "scalar function")
    }

    fn try_encode_udaf(&self, _node: &AggregateUDF, _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        decode_by_name!(self, name, buf, aggregate_by_name, "aggregate function")
    }

    fn try_encode_udwf(&self, _node: &WindowUDF, _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        decode_by_name!(self, name, buf, window_by_name, "window function")
    }
}
