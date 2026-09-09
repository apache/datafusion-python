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

//! This library's own execution plan node.
//!
//! A leaf, and deliberately so. A node with children hands them to the
//! framework to encode with the *host's* codec, which is the right thing but
//! means the interesting part of a codec -- what it writes down -- is somebody
//! else's problem. Everything this node needs to run lives in the node
//! itself: which files, which columns, how many rows. That is what
//! [`crate::codec`] writes to the wire, and it is why a plan built here can be
//! decoded in a process that has never seen this table registered.
//!
//! One output partition per file. That is the axis a distributed engine
//! splits along: partition `i` reads file `i` and nothing else, so two workers
//! never touch the same bytes and no coordination is needed.

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

/// One Parquet file, and the size the object store will report for it.
///
/// The size travels with the path because `PartitionedFile` needs it up front
/// and a decoding process should not have to stat the file to rebuild a plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileSlice {
    pub(crate) path: String,
    pub(crate) size: u64,
}

/// Scans a fixed list of Parquet files, one file per output partition.
#[derive(Debug)]
pub(crate) struct PartitionedParquetExec {
    /// Output partition `i` reads `files[i]`.
    pub(crate) files: Vec<FileSlice>,
    /// The table's full schema, before projection.
    pub(crate) table_schema: SchemaRef,
    /// Column indices into `table_schema`, or `None` for all of them.
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl PartitionedParquetExec {
    pub(crate) fn new(
        files: Vec<FileSlice>,
        table_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Self> {
        let projected_schema = match projection.as_ref() {
            Some(indices) => Arc::new(table_schema.project(indices)?),
            None => Arc::clone(&table_schema),
        };
        // `UnknownPartitioning`, not `Hash`: the rows are split by which file
        // they happen to live in, which says nothing about their values. A
        // plan that claimed a hash partitioning here would let the optimizer
        // skip a repartition it actually needs.
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(files.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            files,
            table_schema,
            projection,
            limit,
            properties,
        })
    }

    /// Build the stock scan for a single one of our files.
    ///
    /// Reusing `DataSourceExec` for the actual reading is the point: this node
    /// exists to own the *description* of the scan across a process boundary,
    /// not to reimplement Parquet.
    fn scan_for(&self, partition: usize) -> Result<Arc<DataSourceExec>> {
        let slice = self.files.get(partition).ok_or_else(|| {
            datafusion::common::internal_datafusion_err!(
                "PartitionedParquetExec has {} partition(s), asked for {partition}",
                self.files.len()
            )
        })?;
        let source = Arc::new(ParquetSource::new(Arc::clone(&self.table_schema)));
        let config = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_file(PartitionedFile::new(slice.path.clone(), slice.size))
            .with_projection_indices(self.projection.clone())?
            .with_limit(self.limit)
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}

impl DisplayAs for PartitionedParquetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PartitionedParquetExec: files={}", self.files.len())?;
        if let Some(projection) = self.projection.as_ref() {
            write!(f, ", projection={projection:?}")?;
        }
        if let Some(limit) = self.limit {
            write!(f, ", limit={limit}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for PartitionedParquetExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // The projection is column indices, not expressions, and any pushed
        // down filter is held by the `DataSourceExec` this node builds.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return datafusion::common::internal_err!(
                "PartitionedParquetExec is a leaf, got {} children",
                children.len()
            );
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Partition 0 of the single-file scan: each of our partitions is one
        // whole file, so the inner scan only ever has one of its own.
        self.scan_for(partition)?.execute(0, context)
    }
}
