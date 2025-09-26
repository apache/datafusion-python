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

use pyo3::prelude::*;
use pyo3::types::*;

use datafusion::config::ConfigOptions;

use crate::errors::PyDataFusionResult;
use crate::utils::py_obj_to_scalar_value;
use parking_lot::RwLock;

#[pyclass(name = "Config", module = "datafusion", subclass, frozen)]
#[derive(Clone)]
pub(crate) struct PyConfig {
    config: Arc<RwLock<ConfigOptions>>,
}

#[pymethods]
impl PyConfig {
    #[new]
    fn py_new() -> Self {
        Self {
            config: Arc::new(RwLock::new(ConfigOptions::new())),
        }
    }

    /// Get configurations from environment variables
    #[staticmethod]
    pub fn from_env() -> PyDataFusionResult<Self> {
        Ok(Self {
            config: Arc::new(RwLock::new(ConfigOptions::from_env()?)),
        })
    }

    /// Get a configuration option
    pub fn get<'py>(&self, key: &str, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let options = self.config.read();
        for entry in options.entries() {
            if entry.key == key {
                return Ok(entry.value.into_pyobject(py)?);
            }
        }
        Ok(None::<String>.into_pyobject(py)?)
    }

    /// Set a configuration option
    pub fn set(&self, key: &str, value: PyObject, py: Python) -> PyDataFusionResult<()> {
        let scalar_value = py_obj_to_scalar_value(py, value)?;
        let mut options = self.config.write();
        options.set(key, scalar_value.to_string().as_str())?;
        Ok(())
    }

    /// Get all configuration options
    pub fn get_all(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        let options = self.config.read();
        for entry in options.entries() {
            dict.set_item(entry.key, entry.value.clone().into_pyobject(py)?)?;
        }
        Ok(dict.into())
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        match self.get_all(py) {
            Ok(result) => Ok(format!("Config({result})")),
            Err(err) => Ok(format!("Error: {:?}", err.to_string())),
        }
    }
}
