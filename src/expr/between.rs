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
use datafusion::logical_expr::expr::Between;
use pyo3::prelude::*;
use std::fmt::{self, Display, Formatter};

#[pyclass(name = "Between", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyBetween {
    between: Between,
}

impl From<PyBetween> for Between {
    fn from(between: PyBetween) -> Self {
        between.between
    }
}

impl From<Between> for PyBetween {
    fn from(between: Between) -> PyBetween {
        PyBetween { between }
    }
}

impl Display for PyBetween {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Between
            Expr: {:?}
            Negated: {:?}
            Low: {:?}
            High: {:?}",
            &self.between.expr, &self.between.negated, &self.between.low, &self.between.high
        )
    }
}

#[pymethods]
impl PyBetween {
    fn expr(&self) -> PyResult<PyExpr> {
        Ok((*self.between.expr).clone().into())
    }

    fn negated(&self) -> PyResult<bool> {
        Ok(self.between.negated)
    }

    fn low(&self) -> PyResult<PyExpr> {
        Ok((*self.between.low).clone().into())
    }

    fn high(&self) -> PyResult<PyExpr> {
        Ok((*self.between.high).clone().into())
    }

    fn __repr__(&self) -> String {
        format!("{self}")
    }
}
