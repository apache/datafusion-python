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

use std::sync::Arc;

use crate::expr::aggregate::PyAggregate;
use crate::expr::analyze::PyAnalyze;
use crate::expr::cross_join::PyCrossJoin;
use crate::expr::distinct::PyDistinct;
use crate::expr::empty_relation::PyEmptyRelation;
use crate::expr::explain::PyExplain;
use crate::expr::extension::PyExtension;
use crate::expr::filter::PyFilter;
use crate::expr::join::PyJoin;
use crate::expr::limit::PyLimit;
use crate::expr::projection::PyProjection;
use crate::expr::sort::PySort;
use crate::expr::subquery::PySubquery;
use crate::expr::subquery_alias::PySubqueryAlias;
use crate::expr::table_scan::PyTableScan;
use crate::expr::unnest::PyUnnest;
use crate::expr::window::PyWindowExpr;
use crate::{context::PySessionContext, errors::py_unsupported_variant_err};
use datafusion::{error::DataFusionError, logical_expr::LogicalPlan};
use datafusion_proto::logical_plan::{AsLogicalPlan, DefaultLogicalExtensionCodec};
use prost::Message;
use pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyBytes};

use crate::expr::logical_node::LogicalNode;

#[pyclass(name = "LogicalPlan", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyLogicalPlan {
    pub(crate) plan: Arc<LogicalPlan>,
}

impl PyLogicalPlan {
    /// creates a new PyLogicalPlan
    pub fn new(plan: LogicalPlan) -> Self {
        Self {
            plan: Arc::new(plan),
        }
    }

    pub fn plan(&self) -> Arc<LogicalPlan> {
        self.plan.clone()
    }
}

#[pymethods]
impl PyLogicalPlan {
    /// Return the specific logical operator
    pub fn to_variant(&self, py: Python) -> PyResult<PyObject> {
        match self.plan.as_ref() {
            LogicalPlan::Aggregate(plan) => PyAggregate::from(plan.clone()).to_variant(py),
            LogicalPlan::Analyze(plan) => PyAnalyze::from(plan.clone()).to_variant(py),
            LogicalPlan::CrossJoin(plan) => PyCrossJoin::from(plan.clone()).to_variant(py),
            LogicalPlan::Distinct(plan) => PyDistinct::from(plan.clone()).to_variant(py),
            LogicalPlan::EmptyRelation(plan) => PyEmptyRelation::from(plan.clone()).to_variant(py),
            LogicalPlan::Explain(plan) => PyExplain::from(plan.clone()).to_variant(py),
            LogicalPlan::Extension(plan) => PyExtension::from(plan.clone()).to_variant(py),
            LogicalPlan::Filter(plan) => PyFilter::from(plan.clone()).to_variant(py),
            LogicalPlan::Join(plan) => PyJoin::from(plan.clone()).to_variant(py),
            LogicalPlan::Limit(plan) => PyLimit::from(plan.clone()).to_variant(py),
            LogicalPlan::Projection(plan) => PyProjection::from(plan.clone()).to_variant(py),
            LogicalPlan::Sort(plan) => PySort::from(plan.clone()).to_variant(py),
            LogicalPlan::TableScan(plan) => PyTableScan::from(plan.clone()).to_variant(py),
            LogicalPlan::Subquery(plan) => PySubquery::from(plan.clone()).to_variant(py),
            LogicalPlan::SubqueryAlias(plan) => PySubqueryAlias::from(plan.clone()).to_variant(py),
            LogicalPlan::Unnest(plan) => PyUnnest::from(plan.clone()).to_variant(py),
            LogicalPlan::Window(plan) => PyWindowExpr::from(plan.clone()).to_variant(py),
            LogicalPlan::Repartition(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Prepare(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::RecursiveQuery(_) => Err(py_unsupported_variant_err(format!(
                "Conversion of variant not implemented: {:?}",
                self.plan
            ))),
        }
    }

    /// Get the inputs to this plan
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        let mut inputs = vec![];
        for input in self.plan.inputs() {
            inputs.push(input.to_owned().into());
        }
        inputs
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.plan))
    }

    fn display(&self) -> String {
        format!("{}", self.plan.display())
    }

    fn display_indent(&self) -> String {
        format!("{}", self.plan.display_indent())
    }

    fn display_indent_schema(&self) -> String {
        format!("{}", self.plan.display_indent_schema())
    }

    fn display_graphviz(&self) -> String {
        format!("{}", self.plan.display_graphviz())
    }

    pub fn to_proto<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let codec = DefaultLogicalExtensionCodec {};
        let proto =
            datafusion_proto::protobuf::LogicalPlanNode::try_from_logical_plan(&self.plan, &codec)?;

        let bytes = proto.encode_to_vec();
        Ok(PyBytes::new_bound(py, &bytes))
    }

    #[staticmethod]
    pub fn from_proto(ctx: PySessionContext, proto_msg: Bound<'_, PyBytes>) -> PyResult<Self> {
        let bytes: &[u8] = proto_msg.extract()?;
        let proto_plan =
            datafusion_proto::protobuf::LogicalPlanNode::decode(bytes).map_err(|e| {
                PyRuntimeError::new_err(format!(
                    "Unable to decode logical node from serialized bytes: {}",
                    e
                ))
            })?;

        let codec = DefaultLogicalExtensionCodec {};
        let plan = proto_plan
            .try_into_logical_plan(&ctx.ctx, &codec)
            .map_err(DataFusionError::from)?;
        Ok(Self::new(plan))
    }
}

impl From<PyLogicalPlan> for LogicalPlan {
    fn from(logical_plan: PyLogicalPlan) -> LogicalPlan {
        logical_plan.plan.as_ref().clone()
    }
}

impl From<LogicalPlan> for PyLogicalPlan {
    fn from(logical_plan: LogicalPlan) -> PyLogicalPlan {
        PyLogicalPlan {
            plan: Arc::new(logical_plan),
        }
    }
}
