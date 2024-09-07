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

use datafusion::logical_expr::expr::InSubquery;
use pyo3::prelude::*;

use super::{subquery::PySubquery, PyExpr};

#[pyclass(name = "InSubquery", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyInSubquery {
    in_subquery: InSubquery,
}

impl From<InSubquery> for PyInSubquery {
    fn from(in_subquery: InSubquery) -> Self {
        PyInSubquery { in_subquery }
    }
}

#[pymethods]
impl PyInSubquery {
    fn expr(&self) -> PyExpr {
        (*self.in_subquery.expr).clone().into()
    }

    fn subquery(&self) -> PySubquery {
        self.in_subquery.subquery.clone().into()
    }

    fn negated(&self) -> bool {
        self.in_subquery.negated
    }
}
