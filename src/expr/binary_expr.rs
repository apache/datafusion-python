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

use datafusion_expr::BinaryExpr;
use pyo3::prelude::*;

#[pyclass(name = "BinaryExpr", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyBinaryExpr {
    expr: BinaryExpr,
}

impl From<PyBinaryExpr> for BinaryExpr {
    fn from(expr: PyBinaryExpr) -> Self {
        expr.expr
    }
}

impl From<BinaryExpr> for PyBinaryExpr {
    fn from(expr: BinaryExpr) -> PyBinaryExpr {
        PyBinaryExpr { expr }
    }
}

#[pymethods]
impl PyBinaryExpr {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.expr))
    }
}
