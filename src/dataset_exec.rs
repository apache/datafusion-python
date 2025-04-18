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

use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
/// Implements a Datafusion physical ExecutionPlan that delegates to a PyArrow Dataset
/// This actually performs the projection, filtering and scanning of a Dataset
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyIterator, PyList};

use std::any::Any;
use std::sync::Arc;

use futures::{stream, TryStreamExt};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError as InnerDataFusionError, Result as DFResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    SendableRecordBatchStream, Statistics,
};

use crate::errors::PyDataFusionResult;
use crate::pyarrow_filter_expression::PyArrowFilterExpression;

struct PyArrowBatchesAdapter {
    batches: Py<PyIterator>,
}

impl Iterator for PyArrowBatchesAdapter {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        Python::with_gil(|py| {
            let mut batches = self.batches.clone_ref(py).into_bound(py);
            Some(
                batches
                    .next()?
                    .and_then(|batch| Ok(batch.extract::<PyArrowType<_>>()?.0))
                    .map_err(|err| ArrowError::ExternalError(Box::new(err))),
            )
        })
    }
}

// Wraps a pyarrow.dataset.Dataset class and implements a Datafusion ExecutionPlan around it
#[derive(Debug)]
pub(crate) struct DatasetExec {
    dataset: PyObject,
    schema: SchemaRef,
    fragments: Py<PyList>,
    columns: Option<Vec<String>>,
    filter_expr: Option<PyObject>,
    projected_statistics: Statistics,
    plan_properties: datafusion::physical_plan::PlanProperties,
}

impl DatasetExec {
    pub fn new(
        py: Python,
        dataset: &Bound<'_, PyAny>,
        projection: Option<Vec<usize>>,
        filters: &[Expr],
    ) -> PyDataFusionResult<Self> {
        let columns: Option<PyDataFusionResult<Vec<String>>> = projection.map(|p| {
            p.iter()
                .map(|index| {
                    let name: String = dataset
                        .getattr("schema")?
                        .call_method1("field", (*index,))?
                        .getattr("name")?
                        .extract()?;
                    Ok(name)
                })
                .collect()
        });
        let columns: Option<Vec<String>> = columns.transpose()?;
        let filter_expr: Option<PyObject> = conjunction(filters.to_owned())
            .map(|filters| {
                PyArrowFilterExpression::try_from(&filters)
                    .map(|filter_expr| filter_expr.inner().clone_ref(py))
            })
            .transpose()?;

        let kwargs = PyDict::new(py);

        kwargs.set_item("columns", columns.clone())?;
        kwargs.set_item(
            "filter",
            filter_expr.as_ref().map(|expr| expr.clone_ref(py)),
        )?;

        let scanner = dataset.call_method("scanner", (), Some(&kwargs))?;

        let schema = Arc::new(
            scanner
                .getattr("projected_schema")?
                .extract::<PyArrowType<_>>()?
                .0,
        );

        let builtins = Python::import(py, "builtins")?;
        let pylist = builtins.getattr("list")?;

        // Get the fragments or partitions of the dataset
        let fragments_iterator: Bound<'_, PyAny> = dataset.call_method1(
            "get_fragments",
            (filter_expr.as_ref().map(|expr| expr.clone_ref(py)),),
        )?;

        let fragments_iter = pylist.call1((fragments_iterator,))?;
        let fragments = fragments_iter.downcast::<PyList>().map_err(PyErr::from)?;

        let projected_statistics = Statistics::new_unknown(&schema);
        let plan_properties = datafusion::physical_plan::PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(fragments.len()),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Ok(DatasetExec {
            dataset: dataset.clone().unbind(),
            schema,
            fragments: fragments.clone().unbind(),
            columns,
            filter_expr,
            projected_statistics,
            plan_properties,
        })
    }
}

impl ExecutionPlan for DatasetExec {
    fn name(&self) -> &str {
        // [ExecutionPlan::name] docs recommends forwarding to `static_name`
        Self::static_name()
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();
        Python::with_gil(|py| {
            let dataset = self.dataset.bind(py);
            let fragments = self.fragments.bind(py);
            let fragment = fragments
                .get_item(partition)
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?;

            // We need to pass the dataset schema to unify the fragment and dataset schema per PyArrow docs
            let dataset_schema = dataset
                .getattr("schema")
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?;
            let kwargs = PyDict::new(py);
            kwargs
                .set_item("columns", self.columns.clone())
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?;
            kwargs
                .set_item(
                    "filter",
                    self.filter_expr.as_ref().map(|expr| expr.clone_ref(py)),
                )
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?;
            kwargs
                .set_item("batch_size", batch_size)
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?;
            let scanner = fragment
                .call_method("scanner", (dataset_schema,), Some(&kwargs))
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?;
            let schema: SchemaRef = Arc::new(
                scanner
                    .getattr("projected_schema")
                    .and_then(|schema| Ok(schema.extract::<PyArrowType<_>>()?.0))
                    .map_err(|err| InnerDataFusionError::External(Box::new(err)))?,
            );
            let record_batches: Bound<'_, PyIterator> = scanner
                .call_method0("to_batches")
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?
                .try_iter()
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?;

            let record_batches = PyArrowBatchesAdapter {
                batches: record_batches.into(),
            };

            let record_batch_stream = stream::iter(record_batches);
            let record_batch_stream: SendableRecordBatchStream = Box::pin(
                RecordBatchStreamAdapter::new(schema, record_batch_stream.map_err(|e| e.into())),
            );
            Ok(record_batch_stream)
        })
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(self.projected_statistics.clone())
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.plan_properties
    }
}

impl ExecutionPlanProperties for DatasetExec {
    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> &Partitioning {
        self.plan_properties.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        None
    }

    fn boundedness(&self) -> Boundedness {
        self.plan_properties.boundedness
    }

    fn pipeline_behavior(&self) -> EmissionType {
        self.plan_properties.emission_type
    }

    fn equivalence_properties(&self) -> &datafusion::physical_expr::EquivalenceProperties {
        &self.plan_properties.eq_properties
    }
}

impl DisplayAs for DatasetExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Python::with_gil(|py| {
            let number_of_fragments = self.fragments.bind(py).len();
            match t {
                DisplayFormatType::Default
                | DisplayFormatType::Verbose
                | DisplayFormatType::TreeRender => {
                    let projected_columns: Vec<String> = self
                        .schema
                        .fields()
                        .iter()
                        .map(|x| x.name().to_owned())
                        .collect();
                    if let Some(filter_expr) = &self.filter_expr {
                        let filter_expr = filter_expr.bind(py).str().or(Err(std::fmt::Error))?;
                        write!(
                            f,
                            "DatasetExec: number_of_fragments={}, filter_expr={}, projection=[{}]",
                            number_of_fragments,
                            filter_expr,
                            projected_columns.join(", "),
                        )
                    } else {
                        write!(
                            f,
                            "DatasetExec: number_of_fragments={}, projection=[{}]",
                            number_of_fragments,
                            projected_columns.join(", "),
                        )
                    }
                }
            }
        })
    }
}
