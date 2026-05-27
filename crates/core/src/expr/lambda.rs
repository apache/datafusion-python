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

use datafusion::logical_expr::expr::Lambda;
use pyo3::prelude::*;

use super::PyExpr;

#[pyclass(
    from_py_object,
    frozen,
    name = "Lambda",
    module = "datafusion.expr",
    subclass
)]
#[derive(Clone)]
pub struct PyLambda {
    lambda: Lambda,
}

impl From<Lambda> for PyLambda {
    fn from(lambda: Lambda) -> PyLambda {
        PyLambda { lambda }
    }
}

impl From<PyLambda> for Lambda {
    fn from(lambda: PyLambda) -> Self {
        lambda.lambda
    }
}

impl Display for PyLambda {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Lambda(params={:?}, body={:?})",
            &self.lambda.params, &self.lambda.body,
        )
    }
}

#[pymethods]
impl PyLambda {
    /// Parameter names of the lambda.
    fn params(&self) -> Vec<String> {
        self.lambda.params.clone()
    }

    /// Body expression of the lambda.
    fn body(&self) -> PyExpr {
        (*self.lambda.body).clone().into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Lambda({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("Lambda".to_string())
    }
}
