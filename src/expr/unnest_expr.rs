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

use datafusion::logical_expr::expr::Unnest;
use pyo3::prelude::*;
use std::fmt::{self, Display, Formatter};

use super::PyExpr;

#[pyclass(name = "UnnestExpr", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyUnnestExpr {
    unnest: Unnest,
}

impl From<Unnest> for PyUnnestExpr {
    fn from(unnest: Unnest) -> PyUnnestExpr {
        PyUnnestExpr { unnest }
    }
}

impl From<PyUnnestExpr> for Unnest {
    fn from(unnest: PyUnnestExpr) -> Self {
        unnest.unnest
    }
}

impl Display for PyUnnestExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Unnest
            Expr: {:?}",
            &self.unnest.expr,
        )
    }
}

#[pymethods]
impl PyUnnestExpr {
    /// Retrieves the expression that is being unnested
    fn expr(&self) -> PyResult<PyExpr> {
        Ok((*self.unnest.expr).clone().into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("UnnestExpr({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("UnnestExpr".to_string())
    }
}
