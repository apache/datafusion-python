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

use arrow::array::{Array, ArrayData};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::scalar::ScalarValue;
use pyo3::types::{PyAnyMethods, PyList};
use pyo3::{Bound, FromPyObject, PyAny, PyObject, PyResult, Python};

use crate::common::data_type::PyScalarValue;
use crate::errors::PyDataFusionError;

impl FromPyArrow for PyScalarValue {
    fn from_pyarrow_bound(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let py = value.py();
        let typ = value.getattr("type")?;
        let val = value.call_method0("as_py")?;

        // construct pyarrow array from the python value and pyarrow type
        let factory = py.import("pyarrow")?.getattr("array")?;
        let args = PyList::new(py, [val])?;
        let array = factory.call1((args, typ))?;

        // convert the pyarrow array to rust array using C data interface
        let array = arrow::array::make_array(ArrayData::from_pyarrow_bound(&array)?);
        let scalar = ScalarValue::try_from_array(&array, 0).map_err(PyDataFusionError::from)?;

        Ok(PyScalarValue(scalar))
    }
}

impl<'source> FromPyObject<'source> for PyScalarValue {
    fn extract_bound(value: &Bound<'source, PyAny>) -> PyResult<Self> {
        Self::from_pyarrow_bound(value)
    }
}

pub fn scalar_to_pyarrow(scalar: &ScalarValue, py: Python) -> PyResult<PyObject> {
    let array = scalar.to_array().map_err(PyDataFusionError::from)?;
    // convert to pyarrow array using C data interface
    let pyarray = array.to_data().to_pyarrow(py)?;
    let pyscalar = pyarray.call_method1(py, "__getitem__", (0,))?;

    Ok(pyscalar)
}
