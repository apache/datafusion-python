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
use datafusion::logical_expr::Case;
use pyo3::prelude::*;

#[pyclass(name = "Case", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyCase {
    case: Case,
}

impl From<PyCase> for Case {
    fn from(case: PyCase) -> Self {
        case.case
    }
}

impl From<Case> for PyCase {
    fn from(case: Case) -> PyCase {
        PyCase { case }
    }
}

#[pymethods]
impl PyCase {
    fn expr(&self) -> Option<PyExpr> {
        self.case.expr.as_ref().map(|e| (**e).clone().into())
    }

    fn when_then_expr(&self) -> Vec<(PyExpr, PyExpr)> {
        self.case
            .when_then_expr
            .iter()
            .map(|e| ((*e.0).clone().into(), (*e.1).clone().into()))
            .collect()
    }

    fn else_expr(&self) -> Option<PyExpr> {
        self.case.else_expr.as_ref().map(|e| (**e).clone().into())
    }
}
