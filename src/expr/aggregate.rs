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

use datafusion_common::DataFusionError;
use datafusion_expr::logical_plan::Aggregate;
use pyo3::prelude::*;
use std::fmt::{self, Display, Formatter};

use super::logical_node::LogicalNode;
use crate::common::df_schema::PyDFSchema;
use crate::expr::PyExpr;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "Aggregate", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyAggregate {
    aggregate: Aggregate,
}

impl From<Aggregate> for PyAggregate {
    fn from(aggregate: Aggregate) -> PyAggregate {
        PyAggregate { aggregate }
    }
}

impl TryFrom<PyAggregate> for Aggregate {
    type Error = DataFusionError;

    fn try_from(agg: PyAggregate) -> Result<Self, Self::Error> {
        Ok(agg.aggregate)
    }
}

impl Display for PyAggregate {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Aggregate
            \nGroupBy(s): {:?}
            \nAggregates(s): {:?}
            \nInput: {:?}
            \nProjected Schema: {:?}",
            &self.aggregate.group_expr,
            &self.aggregate.aggr_expr,
            self.aggregate.input,
            self.aggregate.schema
        )
    }
}

#[pymethods]
impl PyAggregate {
    /// Retrieves the grouping expressions for this `Aggregate`
    fn group_by_exprs(&self) -> PyResult<Vec<PyExpr>> {
        Ok(self
            .aggregate
            .group_expr
            .iter()
            .map(|e| PyExpr::from(e.clone()))
            .collect())
    }

    /// Retrieves the aggregate expressions for this `Aggregate`
    fn aggregate_exprs(&self) -> PyResult<Vec<PyExpr>> {
        Ok(self
            .aggregate
            .aggr_expr
            .iter()
            .map(|e| PyExpr::from(e.clone()))
            .collect())
    }

    // Retrieves the input `LogicalPlan` to this `Aggregate` node
    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    // Resulting Schema for this `Aggregate` node instance
    fn schema(&self) -> PyDFSchema {
        (*self.aggregate.schema).clone().into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Aggregate({})", self))
    }
}

impl LogicalNode for PyAggregate {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.aggregate.input).clone())]
    }

    fn to_variant(&self, py: Python) -> PyResult<PyObject> {
        Ok(self.clone().into_py(py))
    }
}
