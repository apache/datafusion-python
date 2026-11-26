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

//! # NOT A PATTERN
//!
//! **Blocked on:** <https://github.com/apache/datafusion/issues/25152>
//!
//! **Delete when:** `FFI_PlanProperties` carries `scheduling_type`, or
//! `ForeignExecutionPlan` gains a reachable `try_to_proto`. Fixing either one
//! retires this module.
//!
//! **Copying this will:** claim every other library's plan nodes, and produce
//! payloads that decode only in the writing process, exactly once each.
//!
//! `DataSourceExec` is this library's own node. The `ForeignExecutionPlan`
//! arm is a workaround, and it is load-bearing: a host physical optimizer
//! rule that runs during a foreign planner's `create_physical_plan` --
//! `EnsureCooperative` always does -- hands the library back a
//! `ForeignExecutionPlan` wrapping the host's `CooperativeExec`. That type has
//! no reachable `try_to_proto`, so nothing can encode it natively and
//! `FFI_QueryPlanner` must serialize the plan it returns. Claiming the opaque
//! wrapper here and parking it in a process-local registry, rather than
//! serializing it, is what lets those plans round-trip at all.
//!
//! The cost is that this codec also claims every *other* library's nodes,
//! since that is the type any node arrives as once it has crossed the
//! boundary -- see `extension_codec_order`. Narrowing [`claims`] to
//! `DataSourceExec` alone makes 31 tests in
//! `datafusion-ffi-query-planner-example` fail because the wrapped plan can
//! no longer be encoded.
//!
//! A library whose planner controls its own physical optimizer rules never
//! sees a foreign node and needs no such arm.
//!
//! Both halves are upstream defects, tracked together in the issue above:
//! `FFI_PlanProperties` carries no `scheduling_type`, so `EnsureCooperative`
//! reads every foreign leaf as non-cooperative and wraps it, and the resulting
//! `ForeignExecutionPlan` then has no way to serialize itself.
//!
//! The `DataSourceExec` arm could use durable metadata and does not, because
//! the registry must exist for the `ForeignExecutionPlan` arm regardless.
//! Splitting the two arms across wire formats costs real code and removes
//! nothing; this module keeps the temporary compromise obvious.
//!
//! The registry is the execution-plan counterpart of the logical codec's
//! provider registry, with the same lifecycle: encoding inserts, decoding
//! removes, so a decode consumes its token and an encode that is never decoded
//! leaks. See [`crate::logical_extension_codec`] for why that is acceptable
//! here and not in a real codec.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ffi::execution_plan::ForeignExecutionPlan;

const EXECUTION_PLAN_TOKEN: &[u8] = b"DFPYEXEP";
static NEXT_EXECUTION_PLAN_ID: AtomicU64 = AtomicU64::new(1);
static EXECUTION_PLANS: OnceLock<Mutex<HashMap<u64, Arc<dyn ExecutionPlan>>>> = OnceLock::new();

fn execution_plans() -> &'static Mutex<HashMap<u64, Arc<dyn ExecutionPlan>>> {
    EXECUTION_PLANS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn token_id(buf: &[u8]) -> Option<u64> {
    let id: [u8; 8] = buf.strip_prefix(EXECUTION_PLAN_TOKEN)?.try_into().ok()?;
    Some(u64::from_le_bytes(id))
}

pub(crate) fn claims(node: &Arc<dyn ExecutionPlan>) -> bool {
    node.is::<DataSourceExec>() || node.is::<ForeignExecutionPlan>()
}

pub(crate) fn park(node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
    let id = NEXT_EXECUTION_PLAN_ID.fetch_add(1, Ordering::SeqCst);
    execution_plans()
        .lock()
        .map_err(|err| DataFusionError::Internal(err.to_string()))?
        .insert(id, node);
    buf.extend_from_slice(EXECUTION_PLAN_TOKEN);
    buf.extend_from_slice(&id.to_le_bytes());
    Ok(())
}

pub(crate) fn take(buf: &[u8]) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(id) = token_id(buf) else {
        return Ok(None);
    };
    let plan = execution_plans()
        .lock()
        .map_err(|err| DataFusionError::Internal(err.to_string()))?
        .remove(&id)
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Unknown datafusion-ffi-example execution plan token {id}"
            ))
        })?;
    Ok(Some(plan))
}
