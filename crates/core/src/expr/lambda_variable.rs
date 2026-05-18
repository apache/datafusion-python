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

use datafusion::logical_expr::expr::LambdaVariable;
use pyo3::prelude::*;

#[pyclass(
    from_py_object,
    frozen,
    name = "LambdaVariable",
    module = "datafusion.expr",
    subclass
)]
#[derive(Clone)]
pub struct PyLambdaVariable {
    variable: LambdaVariable,
}

impl From<LambdaVariable> for PyLambdaVariable {
    fn from(variable: LambdaVariable) -> PyLambdaVariable {
        PyLambdaVariable { variable }
    }
}

impl From<PyLambdaVariable> for LambdaVariable {
    fn from(variable: PyLambdaVariable) -> Self {
        variable.variable
    }
}

impl Display for PyLambdaVariable {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "LambdaVariable({})", &self.variable.name)
    }
}

#[pymethods]
impl PyLambdaVariable {
    /// Reference name of the lambda parameter.
    fn name(&self) -> String {
        self.variable.name.clone()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("LambdaVariable({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("LambdaVariable".to_string())
    }
}
