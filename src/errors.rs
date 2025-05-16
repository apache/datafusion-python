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

use core::fmt;
use std::error::Error;
use std::fmt::Debug;

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError as InnerDataFusionError;
use prost::EncodeError;
use pyo3::{exceptions::PyException, PyErr};

pub type PyDataFusionResult<T> = std::result::Result<T, PyDataFusionError>;

#[derive(Debug)]
pub enum PyDataFusionError {
    ExecutionError(Box<InnerDataFusionError>),
    ArrowError(ArrowError),
    Common(String),
    PythonError(PyErr),
    EncodeError(EncodeError),
}

impl fmt::Display for PyDataFusionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PyDataFusionError::ExecutionError(e) => write!(f, "DataFusion error: {e:?}"),
            PyDataFusionError::ArrowError(e) => write!(f, "Arrow error: {e:?}"),
            PyDataFusionError::PythonError(e) => write!(f, "Python error {e:?}"),
            PyDataFusionError::Common(e) => write!(f, "{e}"),
            PyDataFusionError::EncodeError(e) => write!(f, "Failed to encode substrait plan: {e}"),
        }
    }
}

impl From<ArrowError> for PyDataFusionError {
    fn from(err: ArrowError) -> PyDataFusionError {
        PyDataFusionError::ArrowError(err)
    }
}

impl From<InnerDataFusionError> for PyDataFusionError {
    fn from(err: InnerDataFusionError) -> PyDataFusionError {
        PyDataFusionError::ExecutionError(Box::new(err))
    }
}

impl From<PyErr> for PyDataFusionError {
    fn from(err: PyErr) -> PyDataFusionError {
        PyDataFusionError::PythonError(err)
    }
}

impl From<PyDataFusionError> for PyErr {
    fn from(err: PyDataFusionError) -> PyErr {
        match err {
            PyDataFusionError::PythonError(py_err) => py_err,
            _ => PyException::new_err(err.to_string()),
        }
    }
}

impl Error for PyDataFusionError {}

pub fn py_type_err(e: impl Debug) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!("{e:?}"))
}

pub fn py_runtime_err(e: impl Debug) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}"))
}

pub fn py_datafusion_err(e: impl Debug) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e:?}"))
}

pub fn py_unsupported_variant_err(e: impl Debug) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{e:?}"))
}

pub fn to_datafusion_err(e: impl Debug) -> InnerDataFusionError {
    InnerDataFusionError::Execution(format!("{e:?}"))
}
