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
use pyo3::prelude::*;

use parking_lot::{Mutex, MutexGuard};

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
    fn lock_case_builder(&self) -> MutexGuard<'_, Option<CaseBuilder>> {
        self.case_builder.lock()
    }

    fn take_case_builder(&self) -> PyDataFusionResult<CaseBuilder> {
        let mut guard = self.lock_case_builder();
        guard.take().ok_or_else(|| {
            PyDataFusionError::Common("CaseBuilder has already been consumed".to_string())
        })
    }

    fn store_case_builder(&self, builder: CaseBuilder) {
        let mut guard = self.lock_case_builder();
        *guard = Some(builder);
    }

    pub fn into_case_builder(self) -> PyDataFusionResult<CaseBuilder> {
        let mut guard = self.case_builder.lock();
        guard.take().ok_or_else(|| {
            PyDataFusionError::Common("CaseBuilder has already been consumed".to_string())
        })
    }
}

#[pymethods]
impl PyCaseBuilder {
    fn when(&self, when: PyExpr, then: PyExpr) -> PyDataFusionResult<PyCaseBuilder> {
        let mut builder = self.take_case_builder()?;
        let next_builder = builder.when(when.expr, then.expr);
        self.store_case_builder(builder);
        Ok(next_builder.into())
    }

    fn otherwise(&self, else_expr: PyExpr) -> PyDataFusionResult<PyExpr> {
        let mut builder = self.take_case_builder()?;
        match builder.otherwise(else_expr.expr) {
            Ok(expr) => {
                self.store_case_builder(builder);
                Ok(expr.clone().into())
            }
            Err(err) => {
                self.store_case_builder(builder);
                Err(err.into())
            }
        }
    }

    fn end(&self) -> PyDataFusionResult<PyExpr> {
        let builder = self.take_case_builder()?;
        match builder.end() {
            Ok(expr) => {
                self.store_case_builder(builder);
                Ok(expr.clone().into())
            }
            Err(err) => {
                self.store_case_builder(builder);
                Err(err.into())
            }
        }
    }
}
