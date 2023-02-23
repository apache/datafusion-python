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
use datafusion_expr::Expr;
use pyo3::prelude::*;

#[pyclass(name = "InList", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyInList {
    expr: Box<Expr>,
    list: Vec<Expr>,
    negated: bool,
}

impl PyInList {
    pub fn new(expr: Box<Expr>, list: Vec<Expr>, negated: bool) -> Self {
        Self {
            expr,
            list,
            negated,
        }
    }
}

#[pymethods]
impl PyInList {
    fn expr(&self) -> PyExpr {
        (*self.expr).clone().into()
    }

    fn list(&self) -> Vec<PyExpr> {
        self.list.iter().map(|e| e.clone().into()).collect()
    }

    fn negated(&self) -> bool {
        self.negated
    }
}
