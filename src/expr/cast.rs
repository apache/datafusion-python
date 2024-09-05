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

use crate::{common::data_type::PyDataType, expr::PyExpr};
use datafusion::logical_expr::{Cast, TryCast};
use pyo3::prelude::*;

#[pyclass(name = "Cast", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyCast {
    cast: Cast,
}

impl From<PyCast> for Cast {
    fn from(cast: PyCast) -> Self {
        cast.cast
    }
}

impl From<Cast> for PyCast {
    fn from(cast: Cast) -> PyCast {
        PyCast { cast }
    }
}

#[pymethods]
impl PyCast {
    fn expr(&self) -> PyResult<PyExpr> {
        Ok((*self.cast.expr).clone().into())
    }

    fn data_type(&self) -> PyResult<PyDataType> {
        Ok(self.cast.data_type.clone().into())
    }
}

#[pyclass(name = "TryCast", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyTryCast {
    try_cast: TryCast,
}

impl From<PyTryCast> for TryCast {
    fn from(try_cast: PyTryCast) -> Self {
        try_cast.try_cast
    }
}

impl From<TryCast> for PyTryCast {
    fn from(try_cast: TryCast) -> PyTryCast {
        PyTryCast { try_cast }
    }
}

#[pymethods]
impl PyTryCast {
    fn expr(&self) -> PyResult<PyExpr> {
        Ok((*self.try_cast.expr).clone().into())
    }

    fn data_type(&self) -> PyResult<PyDataType> {
        Ok(self.try_cast.data_type.clone().into())
    }
}
