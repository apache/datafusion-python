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

use datafusion::logical_expr::conditional_expressions::CaseBuilder;
use datafusion::prelude::Expr;
use pyo3::prelude::*;

use crate::errors::PyDataFusionResult;
use crate::expr::PyExpr;

// TODO(tsaucer) replace this all with CaseBuilder after it implements Clone
#[derive(Clone, Debug)]
#[pyclass(name = "CaseBuilder", module = "datafusion.expr", subclass, frozen)]
pub struct PyCaseBuilder {
    expr: Option<Expr>,
    when: Vec<Expr>,
    then: Vec<Expr>,
}

#[pymethods]
impl PyCaseBuilder {
    #[new]
    pub fn new(expr: Option<PyExpr>) -> Self {
        Self {
            expr: expr.map(Into::into),
            when: vec![],
            then: vec![],
        }
    }

    pub fn when(&self, when: PyExpr, then: PyExpr) -> PyCaseBuilder {
        let mut case_builder = self.clone();
        case_builder.when.push(when.into());
        case_builder.then.push(then.into());

        case_builder
    }

    fn otherwise(&self, else_expr: PyExpr) -> PyDataFusionResult<PyExpr> {
        let case_builder = CaseBuilder::new(
            self.expr.clone().map(Box::new),
            self.when.clone(),
            self.then.clone(),
            Some(Box::new(else_expr.into())),
        );

        let expr = case_builder.end()?;

        Ok(expr.into())
    }

    fn end(&self) -> PyDataFusionResult<PyExpr> {
        let case_builder = CaseBuilder::new(
            self.expr.clone().map(Box::new),
            self.when.clone(),
            self.then.clone(),
            None,
        );

        let expr = case_builder.end()?;

        Ok(expr.into())
    }
}
