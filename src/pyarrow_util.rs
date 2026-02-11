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

//! Conversions between PyArrow and DataFusion types

use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayData, ArrayRef, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::Field;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::common::exec_err;
use datafusion::scalar::ScalarValue;
use pyo3::types::{PyAnyMethods, PyList};
use pyo3::{Bound, FromPyObject, PyAny, PyResult, Python};

use crate::common::data_type::PyScalarValue;
use crate::errors::PyDataFusionError;

fn array_to_scalar_value(array: ArrayRef, as_list_array: bool) -> PyResult<PyScalarValue> {
    if as_list_array {
        let field = Arc::new(Field::new_list_field(
            array.data_type().clone(),
            array.nulls().is_some(),
        ));
        let offsets = OffsetBuffer::from_lengths(vec![array.len()]);
        let list_array = ListArray::new(field, offsets, array, None);
        Ok(PyScalarValue(ScalarValue::List(Arc::new(list_array))))
    } else {
        let scalar = ScalarValue::try_from_array(&array, 0).map_err(PyDataFusionError::from)?;
        Ok(PyScalarValue(scalar))
    }
}

fn pyobj_extract_scalar_via_capsule(
    value: &Bound<'_, PyAny>,
    as_list_array: bool,
) -> PyResult<PyScalarValue> {
    let array_data = ArrayData::from_pyarrow_bound(value)?;
    let array = make_array(array_data);

    array_to_scalar_value(array, as_list_array)
}

impl FromPyArrow for PyScalarValue {
    fn from_pyarrow_bound(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let py = value.py();
        let pyarrow_mod = py.import("pyarrow");

        // Is it a PyArrow object?
        if let Ok(pa) = pyarrow_mod.as_ref() {
            let scalar_type = pa.getattr("Scalar")?;
            if value.is_instance(&scalar_type)? {
                let typ = value.getattr("type")?;

                // construct pyarrow array from the python value and pyarrow type
                let factory = py.import("pyarrow")?.getattr("array")?;
                let args = PyList::new(py, [value])?;
                let array = factory.call1((args, typ))?;

                return pyobj_extract_scalar_via_capsule(&array, false);
            }

            let array_type = pa.getattr("Array")?;
            if value.is_instance(&array_type)? {
                return pyobj_extract_scalar_via_capsule(value, true);
            }
        }

        // Is it a NanoArrow scalar?
        if let Ok(na) = py.import("nanoarrow") {
            let scalar_type = py.import("nanoarrow.array")?.getattr("Scalar")?;
            if value.is_instance(&scalar_type)? {
                return pyobj_extract_scalar_via_capsule(value, false);
            }
            let array_type = na.getattr("Array")?;
            if value.is_instance(&array_type)? {
                return pyobj_extract_scalar_via_capsule(value, true);
            }
        }

        // Is it a arro3 scalar?
        if let Ok(arro3) = py.import("arro3").and_then(|arro3| arro3.getattr("core")) {
            let scalar_type = arro3.getattr("Scalar")?;
            if value.is_instance(&scalar_type)? {
                return pyobj_extract_scalar_via_capsule(value, false);
            }
            let array_type = arro3.getattr("Array")?;
            if value.is_instance(&array_type)? {
                return pyobj_extract_scalar_via_capsule(value, true);
            }
        }

        // Does it have a PyCapsule interface but isn't one of our known libraries?
        // If so do our "best guess". Try checking type name, and if that fails
        // return a single value if the length is 1 and return a List value otherwise
        if value.hasattr("__arrow_c_array__")? {
            let type_name = value.get_type().repr()?;
            if type_name.contains("Scalar")? {
                return pyobj_extract_scalar_via_capsule(value, false);
            }
            if type_name.contains("Array")? {
                return pyobj_extract_scalar_via_capsule(value, true);
            }

            let array_data = ArrayData::from_pyarrow_bound(value)?;
            let array = make_array(array_data);

            let as_array_list = array.len() != 1;
            return array_to_scalar_value(array, as_array_list);
        }

        // Last attempt - try to create a PyArrow scalar from a plain Python object
        if let Ok(pa) = pyarrow_mod.as_ref() {
            let scalar = pa.call_method1("scalar", (value,))?;

            PyScalarValue::from_pyarrow_bound(&scalar)
        } else {
            exec_err!("Unable to import scalar value").map_err(PyDataFusionError::from)?
        }
    }
}

impl<'source> FromPyObject<'source> for PyScalarValue {
    fn extract_bound(value: &Bound<'source, PyAny>) -> PyResult<Self> {
        Self::from_pyarrow_bound(value)
    }
}

pub fn scalar_to_pyarrow<'py>(
    scalar: &ScalarValue,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let array = scalar.to_array().map_err(PyDataFusionError::from)?;
    // convert to pyarrow array using C data interface
    let pyarray = array.to_data().to_pyarrow(py)?;
    let pyscalar = pyarray.call_method1("__getitem__", (0,))?;

    Ok(pyscalar)
}
