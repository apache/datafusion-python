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
use pyo3::prelude::*;
use std::fmt::{self, Display, Formatter};

use datafusion::logical_expr::expr::Alias;

#[pyclass(name = "Alias", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyAlias {
    alias: Alias,
}

impl From<Alias> for PyAlias {
    fn from(alias: Alias) -> Self {
        Self { alias }
    }
}

impl From<PyAlias> for Alias {
    fn from(py_alias: PyAlias) -> Self {
        py_alias.alias
    }
}

impl Display for PyAlias {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Alias
            \nExpr: `{:?}`
            \nAlias Name: `{}`",
            &self.alias.expr, &self.alias.name
        )
    }
}

#[pymethods]
impl PyAlias {
    /// Retrieve the "name" of the alias
    fn alias(&self) -> PyResult<String> {
        Ok(self.alias.name.clone())
    }

    fn expr(&self) -> PyResult<PyExpr> {
        Ok((*self.alias.expr.clone()).into())
    }

    /// Get a String representation of this column
    fn __repr__(&self) -> String {
        format!("{self}")
    }
}
