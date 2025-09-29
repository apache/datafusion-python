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

use std::sync::Arc;

use crate::{
    errors::{PyDataFusionError, PyDataFusionResult},
    expr::PyExpr,
};
use datafusion::logical_expr::conditional_expressions::CaseBuilder;
use parking_lot::{Mutex, MutexGuard};
use pyo3::prelude::*;

struct CaseBuilderHandle<'a> {
    guard: MutexGuard<'a, Option<CaseBuilder>>,
    builder: Option<CaseBuilder>,
}

impl<'a> CaseBuilderHandle<'a> {
    fn new(mut guard: MutexGuard<'a, Option<CaseBuilder>>) -> PyDataFusionResult<Self> {
        let builder = guard.take().ok_or_else(|| {
            PyDataFusionError::Common("CaseBuilder has already been consumed".to_string())
        })?;

        Ok(Self {
            guard,
            builder: Some(builder),
        })
    }

    fn builder_mut(&mut self) -> &mut CaseBuilder {
        self.builder
            .as_mut()
            .expect("builder should be present while handle is alive")
    }

    fn into_inner(mut self) -> CaseBuilder {
        self.builder
            .take()
            .expect("builder should be present when consuming handle")
    }
}

impl Drop for CaseBuilderHandle<'_> {
    fn drop(&mut self) {
        if let Some(builder) = self.builder.take() {
            *self.guard = Some(builder);
        }
    }
}

#[pyclass(name = "CaseBuilder", module = "datafusion.expr", subclass, frozen)]
#[derive(Clone)]
pub struct PyCaseBuilder {
    case_builder: Arc<Mutex<Option<CaseBuilder>>>,
}

impl From<CaseBuilder> for PyCaseBuilder {
    fn from(case_builder: CaseBuilder) -> PyCaseBuilder {
        PyCaseBuilder {
            case_builder: Arc::new(Mutex::new(Some(case_builder))),
        }
    }
}

impl PyCaseBuilder {
    fn case_builder_handle(&self) -> PyDataFusionResult<CaseBuilderHandle<'_>> {
        let guard = self.case_builder.lock();
        CaseBuilderHandle::new(guard)
    }

    pub fn into_case_builder(self) -> PyDataFusionResult<CaseBuilder> {
        let guard = self.case_builder.lock();
        CaseBuilderHandle::new(guard).map(CaseBuilderHandle::into_inner)
    }
}

#[pymethods]
impl PyCaseBuilder {
    fn when(&self, when: PyExpr, then: PyExpr) -> PyDataFusionResult<PyCaseBuilder> {
        let mut handle = self.case_builder_handle()?;
        let next_builder = handle.builder_mut().when(when.expr, then.expr);
        Ok(next_builder.into())
    }

    fn otherwise(&self, else_expr: PyExpr) -> PyDataFusionResult<PyExpr> {
        let mut handle = self.case_builder_handle()?;
        match handle.builder_mut().otherwise(else_expr.expr) {
            Ok(expr) => Ok(expr.clone().into()),
            Err(err) => Err(err.into()),
        }
    }

    fn end(&self) -> PyDataFusionResult<PyExpr> {
        let mut handle = self.case_builder_handle()?;
        match handle.builder_mut().end() {
            Ok(expr) => Ok(expr.clone().into()),
            Err(err) => Err(err.into()),
        }
    }
}
