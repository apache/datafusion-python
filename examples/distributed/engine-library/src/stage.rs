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

//! The node that makes a subtree a unit of remote work.
//!
//! One node does both halves of a shuffle, which is what keeps this example
//! small enough to read. `execute(i)` looks for the file a worker would have
//! written for partition `i`; if it is there it streams it, and if it is not
//! it runs the child instead.
//!
//! That is not a fallback bolted on -- it is what lets *the same node* be the
//! thing the worker runs and the thing the driver reads:
//!
//! - The driver ships this node to a worker. The worker's shuffle directory is
//!   empty, so the node computes its child, and the worker writes the result
//!   to the file for its partition.
//! - The driver then executes the very same plan. The files now exist, so the
//!   node streams them instead of recomputing.
//!
//! Nothing has to rewrite the plan between those two steps, and a query run
//! with no workers at all still produces the right answer -- it just computes
//! everything locally. The shuffle directory travels inside the node, and so
//! inside its encoding, which is what stops a worker and a driver disagreeing
//! about where results go.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fmt, fs};

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, exec_datafusion_err, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;

/// Where the results of one stage partition live.
///
/// Both halves of the exchange are in this file, so the convention has one
/// definition. It is also exported to Python, where the driver uses it to see
/// which partitions have been produced without having to know the layout.
pub(crate) fn partition_path(shuffle_dir: &str, stage_id: u32, partition: usize) -> PathBuf {
    Path::new(shuffle_dir).join(format!("stage-{stage_id}-part-{partition}.arrow"))
}

/// Where a writer builds a partition before publishing it.
///
/// Unique per writer, not merely per partition. Deriving the temporary name
/// from the final one alone would have two writers of the same partition
/// interleave their batches into one file, and then have one of the renames
/// fail because the other already consumed it -- so the rename that is
/// supposed to make publishing atomic would instead be the thing that broke.
/// The driver dispatches each partition once, but a node that is safe only
/// because of how its caller schedules work is not safe.
fn temp_partition_path(shuffle_dir: &str, stage_id: u32, partition: usize) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let unique = NEXT.fetch_add(1, Ordering::Relaxed);
    Path::new(shuffle_dir).join(format!(
        "stage-{stage_id}-part-{partition}.{}-{unique}.arrow.tmp",
        std::process::id()
    ))
}

/// Marks a subtree as one stage of a distributed query.
#[derive(Debug)]
pub(crate) struct ShuffleStageExec {
    pub(crate) stage_id: u32,
    pub(crate) shuffle_dir: String,
    pub(crate) input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl ShuffleStageExec {
    pub(crate) fn new(stage_id: u32, shuffle_dir: String, input: Arc<dyn ExecutionPlan>) -> Self {
        // Reading the child's results back yields the child's partitioning:
        // one file per partition, in partition order.
        let properties = Arc::clone(input.properties());
        Self {
            stage_id,
            shuffle_dir,
            input,
            properties,
        }
    }

    /// Compute this partition and write it where a reader will look.
    ///
    /// The batches are collected before anything is written, because an Arrow
    /// IPC stream needs a schema up front and the file has to be complete
    /// before it is published. A production engine would stream to the file
    /// and track completion separately; holding one partition in memory is
    /// the simplification this example makes.
    ///
    /// Published by rename, so a reader can never observe a half-written
    /// file. The driver waits for workers to exit before reading, but relying
    /// on that alone would break for anyone who overlapped the two. The
    /// writer side of the same argument is why [`temp_partition_path`] is
    /// unique per writer rather than per partition.
    fn write_partition(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = Arc::clone(&self.input);
        let schema = input.schema();
        let final_path = partition_path(&self.shuffle_dir, self.stage_id, partition);
        let temp_path = temp_partition_path(&self.shuffle_dir, self.stage_id, partition);
        let shuffle_dir = self.shuffle_dir.clone();
        // The schema the file is written with is the one this stream declares,
        // not the one the child's stream happens to report. They agree for any
        // well-behaved child, and pinning it to the declaration is what makes
        // a disagreement loud: `StreamWriter::write` rejects a batch whose
        // schema differs, so a child that contradicts its own `schema()` fails
        // here instead of publishing a file readers were told to expect
        // something else from.
        let written_schema = Arc::clone(&schema);

        let collected = async move {
            let mut stream = input.execute(partition, context)?;
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                batches.push(batch?);
            }

            fs::create_dir_all(&shuffle_dir)
                .map_err(|err| exec_datafusion_err!("dfx_engine: creating {shuffle_dir}: {err}"))?;
            {
                let file = fs::File::create(&temp_path).map_err(|err| {
                    exec_datafusion_err!("dfx_engine: creating {}: {err}", temp_path.display())
                })?;
                let mut writer = StreamWriter::try_new(file, written_schema.as_ref())
                    .map_err(|err| exec_datafusion_err!("dfx_engine: ipc writer: {err}"))?;
                for batch in &batches {
                    writer
                        .write(batch)
                        .map_err(|err| exec_datafusion_err!("dfx_engine: writing batch: {err}"))?;
                }
                writer
                    .finish()
                    .map_err(|err| exec_datafusion_err!("dfx_engine: ipc finish: {err}"))?;
            }
            fs::rename(&temp_path, &final_path).map_err(|err| {
                exec_datafusion_err!("dfx_engine: publishing {}: {err}", final_path.display())
            })?;

            Ok::<_, DataFusionError>(batches)
        };

        // Written on first poll rather than here: `execute` must return
        // promptly, so the work happens when the consumer drives the stream.
        let stream = futures::stream::once(collected)
            .map(|result| match result {
                Ok(batches) => futures::stream::iter(batches.into_iter().map(Ok)).boxed(),
                Err(err) => futures::stream::once(async move { Err(err) }).boxed(),
            })
            .flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn read_partition(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let path = partition_path(&self.shuffle_dir, self.stage_id, partition);
        let file = fs::File::open(&path)
            .map_err(|err| exec_datafusion_err!("dfx_engine: opening {}: {err}", path.display()))?;
        let reader = StreamReader::try_new(file, None)
            .map_err(|err| exec_datafusion_err!("dfx_engine: reading {}: {err}", path.display()))?;
        let schema = reader.schema();
        let batches = reader
            .collect::<arrow::error::Result<Vec<_>>>()
            .map_err(|err| exec_datafusion_err!("dfx_engine: reading {}: {err}", path.display()))?;
        Ok(Box::pin(MemoryStream::try_new(batches, schema, None)?))
    }
}

impl DisplayAs for ShuffleStageExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ShuffleStageExec: stage={}", self.stage_id)
    }
}

impl ExecutionPlan for ShuffleStageExec {
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
        // Owns no expressions of its own; the child holds them all.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // The one `Internal` in this file, and the reason the rest are not:
        // children here come from an optimizer rule, not from a payload. No
        // input a user supplies can reach this, so if it fires the caller has
        // a bug and a bug report is the right advice. Everything driven by
        // bytes or by the filesystem is `Execution`.
        if children.len() != 1 {
            return internal_err!(
                "ShuffleStageExec expects exactly one child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self::new(
            self.stage_id,
            self.shuffle_dir.clone(),
            children.swap_remove(0),
        )))
    }

    /// Read this partition's results if they exist, otherwise compute them
    /// and leave them where the next reader will find them.
    ///
    /// The same code runs on a worker and on the driver, and which branch it
    /// takes is decided by the filesystem rather than by a mode flag:
    ///
    /// - On a worker the file is absent, so the child runs and the output is
    ///   written on the way past.
    /// - On the driver the workers have already been and gone, so the file is
    ///   there and the child is never touched.
    ///
    /// A query with no workers at all takes the second branch on every
    /// partition and still gets the right answer, having done the work itself.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition_path(&self.shuffle_dir, self.stage_id, partition).exists() {
            return self.read_partition(partition);
        }
        self.write_partition(partition, context)
    }
}
