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

use datafusion::common::DataFusionError;
use datafusion::logical_expr::expr::{AggregateFunction, AggregateFunctionParams, Alias};
use datafusion::logical_expr::logical_plan::Aggregate;
use datafusion::logical_expr::Expr;
use pyo3::{prelude::*, IntoPyObjectExt};
use std::fmt::{self, Display, Formatter};

use super::logical_node::LogicalNode;
use crate::common::df_schema::PyDFSchema;
use crate::errors::py_type_err;
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

    /// Returns the inner Aggregate Expr(s)
    pub fn agg_expressions(&self) -> PyResult<Vec<PyExpr>> {
        Ok(self
            .aggregate
            .aggr_expr
            .iter()
            .map(|e| PyExpr::from(e.clone()))
            .collect())
    }

    pub fn agg_func_name(&self, expr: PyExpr) -> PyResult<String> {
        Self::_agg_func_name(&expr.expr)
    }

    pub fn aggregation_arguments(&self, expr: PyExpr) -> PyResult<Vec<PyExpr>> {
        self._aggregation_arguments(&expr.expr)
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
        Ok(format!("Aggregate({self})"))
    }
}

impl PyAggregate {
    #[allow(clippy::only_used_in_recursion)]
    fn _aggregation_arguments(&self, expr: &Expr) -> PyResult<Vec<PyExpr>> {
        match expr {
            // TODO: This Alias logic seems to be returning some strange results that we should investigate
            Expr::Alias(Alias { expr, .. }) => self._aggregation_arguments(expr.as_ref()),
            Expr::AggregateFunction(AggregateFunction {
                func: _,
                params: AggregateFunctionParams { args, .. },
                ..
            }) => Ok(args.iter().map(|e| PyExpr::from(e.clone())).collect()),
            _ => Err(py_type_err(
                "Encountered a non Aggregate type in aggregation_arguments",
            )),
        }
    }

    fn _agg_func_name(expr: &Expr) -> PyResult<String> {
        match expr {
            Expr::Alias(Alias { expr, .. }) => Self::_agg_func_name(expr.as_ref()),
            Expr::AggregateFunction(AggregateFunction { func, .. }) => Ok(func.name().to_owned()),
            _ => Err(py_type_err(
                "Encountered a non Aggregate type in agg_func_name",
            )),
        }
    }
}

impl LogicalNode for PyAggregate {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.aggregate.input).clone())]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
