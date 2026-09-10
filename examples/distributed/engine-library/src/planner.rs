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
//! always at least one stage and the orchestration in Python has one shape to
//! deal with.
//!
//! There can be more than one. A `UNION ALL` of two `GROUP BY`s, or a join
//! between two of them, puts a partial aggregate in each branch, and the
//! branches are independent subtrees that both want shipping. Each stage
//! therefore gets its own id, assigned in the order a pre-order walk finds
//! them: stages exchange results through paths built from that id, so two
//! stages sharing one would write to the same files and race each other.

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

/// Id of the first stage in a plan. Later ones count up from here.
///
/// Numbering is a *convention shared with Python*, which cannot read a stage
/// id back off a plan: a foreign node's one-line display is replaced by the
/// FFI wrapper's, so `ShuffleStageExec: stage=2` never reaches the driver.
/// Both sides instead agree that the nth stage found in a pre-order walk has
/// id `FIRST_STAGE_ID + n`, and [`crate::stage_id`] is the one place that
/// arithmetic is written down.
pub(crate) const FIRST_STAGE_ID: u32 = 1;

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

/// Wrap every topmost partial aggregate, numbering the stages as it goes.
///
/// Returns the rewritten plan and how many stages were inserted. Only the
/// *topmost* partial aggregate on a branch is wrapped -- an aggregate nested
/// inside another stage's subtree already travels with it -- so the stages
/// this produces are always disjoint subtrees.
///
/// `next_id` is threaded through rather than being a global counter, because
/// the driver plans the same query twice: once to ship the stages and once to
/// read their results back. A pre-order walk of a deterministic plan assigns
/// the same ids both times only if the numbering restarts per call.
fn insert_stage(
    plan: Arc<dyn ExecutionPlan>,
    shuffle_dir: &str,
    next_id: &mut u32,
) -> Result<(Arc<dyn ExecutionPlan>, usize)> {
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>()
        && matches!(aggregate.mode(), AggregateMode::Partial)
    {
        let stage_id = *next_id;
        *next_id += 1;
        let stage = ShuffleStageExec::new(stage_id, shuffle_dir.to_string(), Arc::clone(&plan));
        return Ok((Arc::new(stage), 1));
    }

    let mut inserted = 0;
    let mut children = Vec::new();
    for child in plan.children() {
        let (child, child_inserted) = insert_stage(Arc::clone(child), shuffle_dir, next_id)?;
        inserted += child_inserted;
        children.push(child);
    }
    if inserted == 0 {
        return Ok((plan, 0));
    }
    // `Keep`: the replacement is a `ShuffleStageExec` wrapping the node it
    // replaced, and that node takes its properties from its child, so the
    // parent's view of its children is unchanged.
    let options = ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep);
    Ok((plan.replace_children(children, options)?, inserted))
}

/// Holds no `fallback`, and that is the design rather than an omission.
///
/// A planner either delegates or rewrites. Delegating hands physical planning
/// to whoever is underneath, including the host, and brings the plan back as
/// opaque foreign nodes -- which cannot be split, and splitting is the only
/// thing this library exists to do. So the hook is handed a fallback and
/// leaves it alone; see `DfxEngineExtension::__datafusion_session_planner__`.
///
/// Two things worth knowing if you write the layering kind instead. Hold the
/// fallback as an `Option<Arc<dyn QueryPlanner + Send + Sync>>` and call it
/// directly: `Session::create_physical_plan` looks like the way to delegate
/// and is not, because it dispatches through the session's *installed*
/// planner, so calling it from inside that planner recurses until the stack
/// overflows. And `datafusion-ffi-query-planner-example` is the crate that
/// demonstrates layering for real, including how `fallback` nests.
#[derive(Debug)]
pub(crate) struct DistributedQueryPlanner {
    pub(crate) observations: Arc<PlannerObservations>,
}

#[async_trait]
impl QueryPlanner for DistributedQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.observations.plan_calls.fetch_add(1, Ordering::SeqCst);

        // Plan against a session that owns the stock rule set locally instead
        // of reaching back over FFI for the host's. Without this the plan
        // contains `ForeignExecutionPlan` wrappers that cannot be serialized
        // and cannot be rewritten -- and rewriting is the next thing that
        // happens here.
        let local = LocalOptimizerSession::new(session);
        let plan = DefaultPhysicalPlanner::default()
            .create_physical_plan(logical_plan, &local)
            .await?;

        let Some(shuffle_dir) = shuffle_dir_from_options(session.config_options()) else {
            // No shuffle directory configured: leave the plan alone and let it
            // run in this process. An engine that inserted stages with nowhere
            // to put their output would fail at execute time instead.
            return Ok(plan);
        };

        let mut next_id = FIRST_STAGE_ID;
        let (plan, inserted) = insert_stage(plan, &shuffle_dir, &mut next_id)?;

        // Nothing to split at, so the whole plan is the one stage. Counted
        // the same way as the rewritten case, so `stages_inserted` is the
        // number of stages a test can expect to find in the plan.
        let (plan, inserted) = match inserted {
            0 => (
                Arc::new(ShuffleStageExec::new(FIRST_STAGE_ID, shuffle_dir, plan)) as _,
                1,
            ),
            inserted => (plan, inserted),
        };

        self.observations
            .stages_inserted
            .fetch_add(inserted, Ordering::SeqCst);
        Ok(plan)
    }
}
