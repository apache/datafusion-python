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

//! Python-aware extension codecs.
//!
//! [`PythonLogicalCodec`] wraps a user-supplied (or default)
//! [`LogicalExtensionCodec`] and is the codec datafusion-python parks
//! on every `SessionContext`. [`PythonPhysicalCodec`] is the symmetric
//! wrapper around [`PhysicalExtensionCodec`].
//!
//! In PR1 both codecs delegate every call to their `inner` codec. The
//! types exist so that follow-up work (pickle support, Python scalar
//! UDF inline encoding) can add in-band Python payloads without
//! re-plumbing the session field.
//!
//! ## Wire-format magic prefix registry
//!
//! Future in-band Python payloads will be prefixed with an 8-byte
//! magic so the decoder can distinguish them from arbitrary
//! `fun_definition` bytes produced by the default codec or a user FFI
//! codec.
//!
//! | Layer + kind                  | Magic prefix | Status        |
//! | ----------------------------- | ------------ | ------------- |
//! | `PythonLogicalCodec` scalar   | `DFPYUDF1`   | reserved (PR2)|
//! | `PythonLogicalCodec` agg      | `DFPYUDA1`   | reserved      |
//! | `PythonLogicalCodec` window   | `DFPYUDW1`   | reserved      |
//! | `PythonPhysicalCodec` scalar  | `DFPYUDF1`   | reserved (PR2)|
//! | `PythonPhysicalCodec` agg     | `DFPYUDA1`   | reserved      |
//! | `PythonPhysicalCodec` window  | `DFPYUDW1`   | reserved      |
//! | `PythonPhysicalCodec` expr    | `DFPYPE1`    | reserved      |
//! | User FFI extension codec      | user-chosen  | downstream    |
//! | Default codec                 | (none)       | upstream      |
//!
//! Dispatch precedence once in-band payloads land: **Python-inline
//! payload (magic prefix match) → `inner` codec → caller's registry
//! fallback.** User FFI codecs should pick non-colliding prefixes
//! (recommend a `DF` namespace plus a crate-specific suffix).

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::{Result, TableReference};
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan, ScalarUDF};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::{DefaultLogicalExtensionCodec, LogicalExtensionCodec};
use datafusion_proto::physical_plan::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};

/// Reserved magic prefix for an inlined Python scalar UDF payload.
/// Not produced or consumed by PR1; the constant is reserved here so
/// follow-up work has a single definition site.
#[allow(dead_code)]
pub(crate) const PY_SCALAR_UDF_MAGIC: &[u8] = b"DFPYUDF1";

/// `LogicalExtensionCodec` parked on every `SessionContext`. Wraps a
/// composable `inner` codec; PR1 delegates every method straight
/// through. The wrapper exists so follow-up patches can add Python
/// in-band encoding without changing every serializer.
#[derive(Debug)]
pub struct PythonLogicalCodec {
    inner: Arc<dyn LogicalExtensionCodec>,
}

impl PythonLogicalCodec {
    pub fn new(inner: Arc<dyn LogicalExtensionCodec>) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &Arc<dyn LogicalExtensionCodec> {
        &self.inner
    }
}

impl Default for PythonLogicalCodec {
    fn default() -> Self {
        Self::new(Arc::new(DefaultLogicalExtensionCodec {}))
    }
}

impl LogicalExtensionCodec for PythonLogicalCodec {
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
        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.inner.try_encode_table_provider(table_ref, node, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udf(node, buf)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.inner.try_decode_udf(name, buf)
    }
}

/// `PhysicalExtensionCodec` mirror of [`PythonLogicalCodec`]. Same
/// motivation: a stable session field that follow-up patches can layer
/// Python in-band encoding onto.
#[derive(Debug)]
pub struct PythonPhysicalCodec {
    inner: Arc<dyn PhysicalExtensionCodec>,
}

impl PythonPhysicalCodec {
    pub fn new(inner: Arc<dyn PhysicalExtensionCodec>) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &Arc<dyn PhysicalExtensionCodec> {
        &self.inner
    }
}

impl Default for PythonPhysicalCodec {
    fn default() -> Self {
        Self::new(Arc::new(DefaultPhysicalExtensionCodec {}))
    }
}

impl PhysicalExtensionCodec for PythonPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udf(node, buf)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.inner.try_decode_udf(name, buf)
    }
}
