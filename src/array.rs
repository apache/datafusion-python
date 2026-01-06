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

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{Field, FieldRef};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::pyarrow::ToPyArrow;
use pyo3::prelude::{PyAnyMethods, PyCapsuleMethods};
use pyo3::types::PyCapsule;
use pyo3::{pyclass, pymethods, Bound, PyAny, PyResult, Python};

use crate::errors::PyDataFusionResult;
use crate::utils::validate_pycapsule;

/// A Python object which implements the Arrow PyCapsule for importing
/// into other libraries.
#[pyclass(name = "ArrowArrayExportable", module = "datafusion", frozen)]
#[derive(Clone)]
pub struct PyArrowArrayExportable {
    array: ArrayRef,
    field: FieldRef,
}

#[pymethods]
impl PyArrowArrayExportable {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyDataFusionResult<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>)> {
        let field = if let Some(schema_capsule) = requested_schema {
            validate_pycapsule(&schema_capsule, "arrow_schema")?;

            let schema_ptr = unsafe { schema_capsule.reference::<FFI_ArrowSchema>() };
            let desired_field = Field::try_from(schema_ptr)?;

            Arc::new(desired_field)
        } else {
            Arc::clone(&self.field)
        };

        let ffi_schema = FFI_ArrowSchema::try_from(&field)?;
        let schema_capsule = PyCapsule::new(py, ffi_schema, Some(cr"arrow_schema".into()))?;

        let ffi_array = FFI_ArrowArray::new(&self.array.to_data());
        let array_capsule = PyCapsule::new(py, ffi_array, Some(cr"arrow_array".into()))?;

        Ok((schema_capsule, array_capsule))
    }
}

impl ToPyArrow for PyArrowArrayExportable {
    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let module = py.import("pyarrow")?;
        let method = module.getattr("array")?;
        let array = method.call((self.clone(),), None)?;
        Ok(array)
    }
}

impl PyArrowArrayExportable {
    pub fn new(array: ArrayRef, field: FieldRef) -> Self {
        Self { array, field }
    }
}
