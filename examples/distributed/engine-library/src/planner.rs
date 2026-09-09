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

//! Where the plan gets split into stages.
//!
//! The split point is the partial aggregate. DataFusion already breaks a
//! `GROUP BY` into a partial pass per input partition and a final pass that
//! merges them, which is exactly the shape a distributed engine wants: the
//! partial passes are independent, so they can run anywhere, and only their
//! output has to come back. Wrapping the partial aggregate in a
//! [`ShuffleStageExec`] is the whole rewrite.
//!
//! A query with no aggregate gets its whole plan wrapped instead, so there is
//! always exactly one stage and the orchestration in Python has one shape to
//! deal with.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::execution_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_session::{QueryPlanner, Session};

use crate::local_session::LocalOptimizerSession;
use crate::stage::ShuffleStageExec;

/// Config key naming the directory stages exchange results through.
///
/// Read from the session rather than baked in, because the driver picks a
/// fresh directory per query and the workers have to be told the same one.
pub(crate) const SHUFFLE_DIR_KEY: &str = "dfx_engine.shuffle_dir";

/// The same setting once the session has crossed the FFI boundary, where
/// every foreign config extension is namespaced under `datafusion_ffi`.
const FFI_SHUFFLE_DIR_KEY: &str = "datafusion_ffi.dfx_engine.shuffle_dir";

/// The one stage id this engine produces. A real engine would number a chain
/// of them; one is enough to show the mechanism.
pub(crate) const STAGE_ID: u32 = 1;

/// What the planner did, so a test can assert it rather than infer it.
#[derive(Default, Debug)]
pub(crate) struct PlannerObservations {
    pub(crate) plan_calls: AtomicUsize,
    pub(crate) stages_inserted: AtomicUsize,
}

pub(crate) fn shuffle_dir_from_options(options: &ConfigOptions) -> Option<String> {
    options
        .entries()
        .into_iter()
        .find(|entry| entry.key == SHUFFLE_DIR_KEY || entry.key == FFI_SHUFFLE_DIR_KEY)
        .and_then(|entry| entry.value)
        // A registered config extension always *has* an entry, so an unset
        // directory arrives as `Some("")` rather than `None`. Treating that as
        // configured inserts a stage whose paths are relative to whatever the
        // process's working directory happens to be -- which silently writes
        // shuffle files next to the caller and then reads another query's
        // leftovers back out of them.
        .filter(|shuffle_dir| !shuffle_dir.is_empty())
}

/// Wrap the partial aggregate, or the whole plan if there is not one.
///
/// Returns the rewritten plan and whether a stage was inserted. Only the
/// topmost partial aggregate is wrapped: an aggregate nested inside another
/// stage's subtree already travels with it.
fn insert_stage(
    plan: Arc<dyn ExecutionPlan>,
    shuffle_dir: &str,
) -> Result<(Arc<dyn ExecutionPlan>, bool)> {
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>()
        && matches!(aggregate.mode(), AggregateMode::Partial)
    {
        let stage = ShuffleStageExec::new(STAGE_ID, shuffle_dir.to_string(), Arc::clone(&plan));
        return Ok((Arc::new(stage), true));
    }

    let mut inserted = false;
    let mut children = Vec::new();
    for child in plan.children() {
        let (child, child_inserted) = insert_stage(Arc::clone(child), shuffle_dir)?;
        inserted |= child_inserted;
        children.push(child);
    }
    if !inserted {
        return Ok((plan, false));
    }
    // `Keep`: the replacement is a `ShuffleStageExec` wrapping the node it
    // replaced, and that node takes its properties from its child, so the
    // parent's view of its children is unchanged.
    let options = ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep);
    Ok((plan.replace_children(children, options)?, true))
}

#[derive(Debug)]
pub(crate) struct DistributedQueryPlanner {
    pub(crate) observations: Arc<PlannerObservations>,
    /// Planner to layer on top of, if the session already had one.
    ///
    /// Held so several planner-shipping libraries compose. Note that
    /// `Session::create_physical_plan` cannot be used for this: it dispatches
    /// through the session's installed planner, so calling it from inside that
    /// planner recurses until the stack overflows.
    pub(crate) fallback: Option<Arc<dyn QueryPlanner + Send + Sync>>,
}

#[async_trait]
impl QueryPlanner for DistributedQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.observations.plan_calls.fetch_add(1, Ordering::SeqCst);

        let plan = match self.fallback.as_ref() {
            // Delegating hands physical planning to whoever is underneath,
            // including the host. That is correct for composition, but it
            // means the plan comes back as opaque foreign nodes this engine
            // cannot split -- so a fallback and a split are exclusive, and
            // the split is what this library is for.
            Some(fallback) => return fallback.create_physical_plan(logical_plan, session).await,
            None => {
                // Plan against a session that owns the stock rule set locally
                // instead of reaching back over FFI for the host's. Without
                // this the plan contains `ForeignExecutionPlan` wrappers that
                // cannot be serialized and cannot be rewritten.
                let local = LocalOptimizerSession::new(session);
                DefaultPhysicalPlanner::default()
                    .create_physical_plan(logical_plan, &local)
                    .await?
            }
        };

        let Some(shuffle_dir) = shuffle_dir_from_options(session.config_options()) else {
            // No shuffle directory configured: leave the plan alone and let it
            // run in this process. An engine that inserted stages with nowhere
            // to put their output would fail at execute time instead.
            return Ok(plan);
        };

        let (plan, inserted) = insert_stage(plan, &shuffle_dir)?;
        if inserted {
            self.observations
                .stages_inserted
                .fetch_add(1, Ordering::SeqCst);
            return Ok(plan);
        }

        // Nothing to split at, so the whole plan is the stage.
        self.observations
            .stages_inserted
            .fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(ShuffleStageExec::new(STAGE_ID, shuffle_dir, plan)))
    }
}
