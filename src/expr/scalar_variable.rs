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

use arrow::datatypes::FieldRef;
use pyo3::prelude::*;

use crate::common::data_type::PyDataType;

#[pyclass(frozen, name = "ScalarVariable", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyScalarVariable {
    field: FieldRef,
    variables: Vec<String>,
}

impl PyScalarVariable {
    pub fn new(field: &FieldRef, variables: &[String]) -> Self {
        Self {
            field: field.to_owned(),
            variables: variables.to_vec(),
        }
    }
}

#[pymethods]
impl PyScalarVariable {
    /// Get the data type
    fn data_type(&self) -> PyResult<PyDataType> {
        Ok(self.field.data_type().clone().into())
    }

    fn variables(&self) -> PyResult<Vec<String>> {
        Ok(self.variables.clone())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}{:?}", self.field.data_type(), self.variables))
    }
}
