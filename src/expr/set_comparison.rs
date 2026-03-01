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

use datafusion::logical_expr::expr::SetComparison;
use pyo3::prelude::*;

use crate::expr::PyExpr;

use super::subquery::PySubquery;

#[pyclass(
    from_py_object,
    frozen,
    name = "SetComparison",
    module = "datafusion.set_comparison",
    subclass
)]
#[derive(Clone)]
pub struct PySetComparison {
    set_comparison: SetComparison,
}

impl From<SetComparison> for PySetComparison {
    fn from(set_comparison: SetComparison) -> Self {
        PySetComparison { set_comparison }
    }
}

#[pymethods]
impl PySetComparison {
    fn expr(&self) -> PyExpr {
        (*self.set_comparison.expr).clone().into()
    }

    fn subquery(&self) -> PySubquery {
        self.set_comparison.subquery.clone().into()
    }

    fn op(&self) -> String {
        format!("{}", self.set_comparison.op)
    }

    fn quantifier(&self) -> String {
        format!("{}", self.set_comparison.quantifier)
    }
}
