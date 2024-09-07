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
use datafusion::logical_expr::expr::{GetFieldAccess, GetIndexedField};
use pyo3::prelude::*;
use std::fmt::{Display, Formatter};

use super::literal::PyLiteral;

#[pyclass(name = "GetIndexedField", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyGetIndexedField {
    indexed_field: GetIndexedField,
}

impl From<PyGetIndexedField> for GetIndexedField {
    fn from(indexed_field: PyGetIndexedField) -> Self {
        indexed_field.indexed_field
    }
}

impl From<GetIndexedField> for PyGetIndexedField {
    fn from(indexed_field: GetIndexedField) -> PyGetIndexedField {
        PyGetIndexedField { indexed_field }
    }
}

impl Display for PyGetIndexedField {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "GetIndexedField
            Expr: {:?}
            Key: {:?}",
            &self.indexed_field.expr, &self.indexed_field.field
        )
    }
}

#[pymethods]
impl PyGetIndexedField {
    fn expr(&self) -> PyResult<PyExpr> {
        Ok((*self.indexed_field.expr).clone().into())
    }

    fn key(&self) -> PyResult<PyLiteral> {
        match &self.indexed_field.field {
            GetFieldAccess::NamedStructField { name, .. } => Ok(name.clone().into()),
            _ => todo!(),
        }
    }

    /// Get a String representation of this column
    fn __repr__(&self) -> String {
        format!("{}", self)
    }
}
