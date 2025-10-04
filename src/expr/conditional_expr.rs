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
use datafusion::common::{exec_err, DataFusionError};
use datafusion::logical_expr::conditional_expressions::CaseBuilder;
use datafusion::prelude::Expr;
use pyo3::prelude::*;

#[pyclass(name = "CaseBuilder", module = "datafusion.expr", subclass, frozen)]
pub struct PyCaseBuilder {
    case_builder: CaseBuilder,
}

impl From<CaseBuilder> for PyCaseBuilder {
    fn from(case_builder: CaseBuilder) -> PyCaseBuilder {
        PyCaseBuilder { case_builder }
    }
}

// TODO(tsaucer) upstream make CaseBuilder impl Clone
fn builder_clone(case_builder: &CaseBuilder) -> Result<CaseBuilder, DataFusionError> {
    let Expr::Case(case) = case_builder.end()? else {
        return exec_err!("CaseBuilder returned an invalid expression");
    };

    let (when_expr, then_expr) = case
        .when_then_expr
        .iter()
        .map(|(w, t)| (w.as_ref().to_owned(), t.as_ref().to_owned()))
        .unzip();

    Ok(CaseBuilder::new(
        case.expr,
        when_expr,
        then_expr,
        case.else_expr,
    ))
}

#[pymethods]
impl PyCaseBuilder {
    fn when(&self, when: PyExpr, then: PyExpr) -> PyDataFusionResult<PyCaseBuilder> {
        let case_builder = builder_clone(&self.case_builder)?.when(when.expr, then.expr);
        Ok(PyCaseBuilder { case_builder })
    }

    fn otherwise(&self, else_expr: PyExpr) -> PyDataFusionResult<PyExpr> {
        Ok(builder_clone(&self.case_builder)?
            .otherwise(else_expr.expr)?
            .into())
    }

    fn end(&self) -> PyDataFusionResult<PyExpr> {
        Ok(builder_clone(&self.case_builder)?.end()?.into())
    }
}
