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

//! A custom execution plan node owned by this library.
//!
//! This is the half of an extension bundle that makes the other half
//! necessary. A query planner that only rearranges stock DataFusion nodes needs
//! no codec of its own; one that emits a node *it* defines does, because
//! nothing else in the process knows how to serialize it. Shipping the planner
//! and the codec that carries its nodes as one bundle is the normal case, and
//! it is why `with_extensions` installs every codec before it binds any
//! planner.
//!
//! The node itself is deliberately trivial — it passes its child's stream
//! through untouched. A real distributed engine would ship the child plan to a
//! remote executor here; what matters for the example is that the node exists,
//! that this library's planner produces it, and that only this library's codec
//! can encode and decode it.

use std::fmt;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

/// Marks a subtree this library claims for remote execution.
#[derive(Debug)]
pub(crate) struct DistributedExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl DistributedExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        // The node is pass-through, so it inherits its child's properties
        // rather than describing anything of its own.
        let properties = Arc::clone(input.properties());
        Self { input, properties }
    }
}

impl DisplayAs for DistributedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DistributedExec")
    }
}

impl ExecutionPlan for DistributedExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // Owns no physical expressions of its own; the child holds them all.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return datafusion::common::internal_err!(
                "DistributedExec expects exactly one child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self::new(children.swap_remove(0))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}
