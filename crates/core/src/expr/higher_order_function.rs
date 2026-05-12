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

use std::fmt::{self, Display, Formatter};

use datafusion::logical_expr::expr::HigherOrderFunction;
use pyo3::prelude::*;

use super::PyExpr;

#[pyclass(
    from_py_object,
    frozen,
    name = "HigherOrderFunction",
    module = "datafusion.expr",
    subclass
)]
#[derive(Clone)]
pub struct PyHigherOrderFunction {
    higher_order: HigherOrderFunction,
}

impl From<HigherOrderFunction> for PyHigherOrderFunction {
    fn from(higher_order: HigherOrderFunction) -> PyHigherOrderFunction {
        PyHigherOrderFunction { higher_order }
    }
}

impl From<PyHigherOrderFunction> for HigherOrderFunction {
    fn from(higher_order: PyHigherOrderFunction) -> Self {
        higher_order.higher_order
    }
}

impl Display for PyHigherOrderFunction {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "HigherOrderFunction(name={}, args={:?})",
            self.higher_order.name(),
            &self.higher_order.args,
        )
    }
}

#[pymethods]
impl PyHigherOrderFunction {
    /// Name of the higher-order function being invoked.
    fn name(&self) -> String {
        self.higher_order.name().to_string()
    }

    /// Arguments passed to the higher-order function. Some entries may be
    /// `Lambda` expressions; others are ordinary value expressions.
    fn args(&self) -> Vec<PyExpr> {
        self.higher_order
            .args
            .iter()
            .map(|e| PyExpr::from(e.clone()))
            .collect()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("HigherOrderFunction({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("HigherOrderFunction".to_string())
    }
}
