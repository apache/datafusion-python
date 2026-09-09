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

//! Carries this engine's own node.
//!
//! This is the codec half of the bundle, and the reason the bundle ships both
//! halves. The planner emits a [`ShuffleStageExec`]; nothing else in the
//! process knows that type, so without this codec the plans that planner
//! produces cannot be serialized at all -- and an engine whose whole job is
//! sending plans to workers would not get off the ground.
//!
//! The payload is the stage id and the shuffle directory, and nothing else.
//! The child is not encoded here: after `try_encode` returns, the framework
//! encodes `children()` itself using the *host's* chain, so the scan
//! underneath is claimed by whichever library owns it. Claiming a whole
//! subtree would cut those libraries out -- and could not work anyway, since
//! the FFI codec wrapper drops the caller's converter and substitutes a bare
//! default.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::common::{Result, internal_datafusion_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};

use crate::stage::ShuffleStageExec;

/// Framing magic; the trailing digit is the payload version.
const MAGIC: &[u8; 8] = b"DFXENG01";

#[derive(Default, Debug)]
pub(crate) struct CodecCounters {
    pub(crate) encoded: AtomicUsize,
    pub(crate) decoded: AtomicUsize,
}

pub(crate) struct DfxEnginePhysicalCodec {
    inner: DefaultPhysicalExtensionCodec,
    pub(crate) counters: Arc<CodecCounters>,
}

impl DfxEnginePhysicalCodec {
    pub(crate) fn new(counters: Arc<CodecCounters>) -> Self {
        Self {
            inner: DefaultPhysicalExtensionCodec {},
            counters,
        }
    }
}

impl fmt::Debug for DfxEnginePhysicalCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DfxEnginePhysicalCodec")
            .finish_non_exhaustive()
    }
}

impl PhysicalExtensionCodec for DfxEnginePhysicalCodec {
    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        // This engine's own type only. Anything else goes to the default
        // codec, whose error is the chain's "not mine" signal.
        let Some(stage) = node.downcast_ref::<ShuffleStageExec>() else {
            return self.inner.try_encode(node, buf, proto_converter);
        };

        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&stage.stage_id.to_le_bytes());
        buf.extend_from_slice(stage.shuffle_dir.as_bytes());

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
        let Some(rest) = buf.strip_prefix(MAGIC) else {
            return self.inner.try_decode(buf, inputs, ctx, proto_converter);
        };

        let (stage_id, shuffle_dir) = rest.split_at_checked(4).ok_or_else(|| {
            internal_datafusion_err!("dfx_engine: payload truncated before stage id")
        })?;
        let stage_id = u32::from_le_bytes(
            stage_id
                .try_into()
                .map_err(|_| internal_datafusion_err!("dfx_engine: bad stage id"))?,
        );
        let shuffle_dir = std::str::from_utf8(shuffle_dir)
            .map_err(|err| internal_datafusion_err!("dfx_engine: bad shuffle dir: {err}"))?;

        // The child arrives already decoded, by the host's chain.
        let [input] = inputs else {
            return internal_err!(
                "ShuffleStageExec expects exactly one input, got {}",
                inputs.len()
            );
        };

        self.counters.decoded.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(ShuffleStageExec::new(
            stage_id,
            shuffle_dir.to_string(),
            Arc::clone(input),
        )))
    }
}
