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

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DFField, OwnedTableReference};
use pyo3::prelude::*;

use super::data_type::PyDataType;

/// PyDFField wraps an arrow-datafusion `DFField` struct type
/// and also supplies convenience methods for interacting
/// with the `DFField` instance in the context of Python
#[pyclass(name = "DFField", module = "datafusion.common", subclass)]
#[derive(Debug, Clone)]
pub struct PyDFField {
    field: DFField,
}

impl From<PyDFField> for DFField {
    fn from(py_field: PyDFField) -> DFField {
        py_field.field
    }
}

impl From<DFField> for PyDFField {
    fn from(field: DFField) -> PyDFField {
        PyDFField { field }
    }
}

#[pymethods]
impl PyDFField {
    #[new]
    #[pyo3(signature = (qualifier=None, name="", data_type=DataType::Int64.into(), nullable=false))]
    fn new(qualifier: Option<String>, name: &str, data_type: PyDataType, nullable: bool) -> Self {
        PyDFField {
            field: DFField::new(
                qualifier.map(|q| OwnedTableReference::from(q)),
                name,
                data_type.into(),
                nullable,
            ),
        }
    }

    // TODO: Need bindings for Array `Field` first
    // #[staticmethod]
    // #[pyo3(name = "from")]
    // fn py_from(field: Field) -> Self {}

    // TODO: Need bindings for Array `Field` first
    // #[staticmethod]
    // #[pyo3(name = "from_qualified")]
    // fn py_from_qualified(field: Field) -> Self {}

    #[pyo3(name = "name")]
    fn py_name(&self) -> PyResult<String> {
        Ok(self.field.name().clone())
    }

    #[pyo3(name = "data_type")]
    fn py_data_type(&self) -> PyResult<PyDataType> {
        Ok(self.field.data_type().clone().into())
    }

    #[pyo3(name = "is_nullable")]
    fn py_is_nullable(&self) -> PyResult<bool> {
        Ok(self.field.is_nullable())
    }

    #[pyo3(name = "qualified_name")]
    fn py_qualified_name(&self) -> PyResult<String> {
        Ok(self.field.qualified_name())
    }

    // TODO: Need bindings for `Column` first
    // #[pyo3(name = "qualified_column")]
    // fn py_qualified_column(&self) -> PyResult<PyColumn> {}

    // TODO: Need bindings for `Column` first
    // #[pyo3(name = "unqualified_column")]
    // fn py_unqualified_column(&self) -> PyResult<PyColumn> {}

    #[pyo3(name = "qualifier")]
    fn py_qualifier(&self) -> PyResult<Option<String>> {
        Ok(self.field.qualifier().map(|q| format!("{}", q)))
    }

    // TODO: Need bindings for Arrow `Field` first
    // #[pyo3(name = "field")]
    // fn py_field(&self) -> PyResult<Field> {}

    #[pyo3(name = "strip_qualifier")]
    fn py_strip_qualifier(&self) -> PyResult<Self> {
        Ok(self.field.clone().strip_qualifier().into())
    }
}
