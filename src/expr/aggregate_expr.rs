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

use crate::expr::PyExpr;
use datafusion::logical_expr::expr::AggregateFunction;
use pyo3::prelude::*;
use std::fmt::{Display, Formatter};

#[pyclass(name = "AggregateFunction", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyAggregateFunction {
    aggr: AggregateFunction,
}

impl From<PyAggregateFunction> for AggregateFunction {
    fn from(aggr: PyAggregateFunction) -> Self {
        aggr.aggr
    }
}

impl From<AggregateFunction> for PyAggregateFunction {
    fn from(aggr: AggregateFunction) -> PyAggregateFunction {
        PyAggregateFunction { aggr }
    }
}

impl Display for PyAggregateFunction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let args: Vec<String> = self
            .aggr
            .params
            .args
            .iter()
            .map(|expr| expr.to_string())
            .collect();
        write!(f, "{}({})", self.aggr.func.name(), args.join(", "))
    }
}

#[pymethods]
impl PyAggregateFunction {
    /// Get the aggregate type, such as "MIN", or "MAX"
    fn aggregate_type(&self) -> String {
        self.aggr.func.name().to_string()
    }

    /// is this a distinct aggregate such as `COUNT(DISTINCT expr)`
    fn is_distinct(&self) -> bool {
        self.aggr.params.distinct
    }

    /// Get the arguments to the aggregate function
    fn args(&self) -> Vec<PyExpr> {
        self.aggr
            .params
            .args
            .iter()
            .map(|expr| PyExpr::from(expr.clone()))
            .collect()
    }

    /// Get a String representation of this column
    fn __repr__(&self) -> String {
        format!("{self}")
    }
}
