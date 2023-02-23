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
use datafusion_expr::{BuiltinScalarFunction, Expr};
use pyo3::prelude::*;

#[pyclass(name = "ScalarFunction", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyScalarFunction {
    scalar_function: BuiltinScalarFunction,
    args: Vec<Expr>,
}

impl PyScalarFunction {
    pub fn new(scalar_function: BuiltinScalarFunction, args: Vec<Expr>) -> Self {
        Self {
            scalar_function,
            args,
        }
    }
}

#[pyclass(name = "BuiltinScalarFunction", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyBuiltinScalarFunction {
    scalar_function: BuiltinScalarFunction,
}

impl From<BuiltinScalarFunction> for PyBuiltinScalarFunction {
    fn from(scalar_function: BuiltinScalarFunction) -> PyBuiltinScalarFunction {
        PyBuiltinScalarFunction { scalar_function }
    }
}

impl From<PyBuiltinScalarFunction> for BuiltinScalarFunction {
    fn from(scalar_function: PyBuiltinScalarFunction) -> Self {
        scalar_function.scalar_function
    }
}

#[pymethods]
impl PyScalarFunction {
    fn fun(&self) -> PyResult<PyBuiltinScalarFunction> {
        Ok(self.scalar_function.clone().into())
    }

    fn args(&self) -> PyResult<Vec<PyExpr>> {
        Ok(self.args.iter().map(|e| e.clone().into()).collect())
    }
}
