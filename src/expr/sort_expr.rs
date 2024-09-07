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
use datafusion::logical_expr::SortExpr;
use pyo3::prelude::*;
use std::fmt::{self, Display, Formatter};

#[pyclass(name = "SortExpr", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PySortExpr {
    sort: SortExpr,
}

impl From<PySortExpr> for SortExpr {
    fn from(sort: PySortExpr) -> Self {
        sort.sort
    }
}

impl From<SortExpr> for PySortExpr {
    fn from(sort: SortExpr) -> PySortExpr {
        PySortExpr { sort }
    }
}

impl Display for PySortExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Sort
            Expr: {:?}
            Asc: {:?}
            NullsFirst: {:?}",
            &self.sort.expr, &self.sort.asc, &self.sort.nulls_first
        )
    }
}

#[pymethods]
impl PySortExpr {
    fn expr(&self) -> PyResult<PyExpr> {
        Ok((*self.sort.expr).clone().into())
    }

    fn ascending(&self) -> PyResult<bool> {
        Ok(self.sort.asc)
    }

    fn nulls_first(&self) -> PyResult<bool> {
        Ok(self.sort.nulls_first)
    }

    fn __repr__(&self) -> String {
        format!("{}", self)
    }
}
