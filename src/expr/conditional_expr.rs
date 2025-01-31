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

use crate::{errors::PyDataFusionResult, expr::PyExpr};
use datafusion::logical_expr::conditional_expressions::CaseBuilder;
use pyo3::prelude::*;

#[pyclass(name = "CaseBuilder", module = "datafusion.expr", subclass)]
pub struct PyCaseBuilder {
    pub case_builder: CaseBuilder,
}

impl From<PyCaseBuilder> for CaseBuilder {
    fn from(case_builder: PyCaseBuilder) -> Self {
        case_builder.case_builder
    }
}

impl From<CaseBuilder> for PyCaseBuilder {
    fn from(case_builder: CaseBuilder) -> PyCaseBuilder {
        PyCaseBuilder { case_builder }
    }
}

#[pymethods]
impl PyCaseBuilder {
    fn when(&mut self, when: PyExpr, then: PyExpr) -> PyCaseBuilder {
        PyCaseBuilder {
            case_builder: self.case_builder.when(when.expr, then.expr),
        }
    }

    fn otherwise(&mut self, else_expr: PyExpr) -> PyDataFusionResult<PyExpr> {
        Ok(self.case_builder.otherwise(else_expr.expr)?.clone().into())
    }

    fn end(&mut self) -> PyDataFusionResult<PyExpr> {
        Ok(self.case_builder.end()?.clone().into())
    }
}
