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

use datafusion::config::ConfigOptions;
use datafusion_ffi::config::extension_options::FFI_ExtensionOptions;
use parking_lot::RwLock;
use pyo3::prelude::*;
use pyo3::types::*;

use crate::errors::{py_datafusion_err, PyDataFusionResult};
use crate::utils::{py_obj_to_scalar_value, validate_pycapsule};
#[pyclass(name = "Config", module = "datafusion", subclass, frozen)]
#[derive(Clone)]
pub(crate) struct PyConfig {
    config: Arc<RwLock<ConfigOptions>>,
}

/// // set up config struct and register extension
/// let mut config = ConfigOptions::default();
/// config.extensions.insert(MyConfig::default());
///
/// // overwrite config default
/// config.set("my_config.baz_count", "42").unwrap();
///
/// // check config state
/// let my_config = config.extensions.get::<MyConfig>().unwrap();
/// assert!(my_config.foo_to_bar,);
/// assert_eq!(my_config.baz_count, 42,);

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
        let value: Option<Option<String>> = {
            let options = self.config.read();
            options
                .entries()
                .into_iter()
                .find_map(|entry| (entry.key == key).then_some(entry.value.clone()))
        };

        match value {
            Some(value) => Ok(value.into_pyobject(py)?),
            None => Ok(None::<String>.into_pyobject(py)?),
        }
    }

    /// Set a configuration option
    pub fn set(&self, key: &str, value: Py<PyAny>, py: Python) -> PyDataFusionResult<()> {
        let scalar_value = py_obj_to_scalar_value(py, value)?;
        let mut options = self.config.write();
        options.set(key, scalar_value.to_string().as_str())?;
        Ok(())
    }

    /// Get all configuration options
    pub fn get_all(&self, py: Python) -> PyResult<Py<PyAny>> {
        let entries: Vec<(String, Option<String>)> = {
            let options = self.config.read();
            options
                .entries()
                .into_iter()
                .map(|entry| {
                    if entry.key.starts_with("datafusion_ffi.") {
                        (entry.key[15..].to_owned(), entry.value.to_owned())
                    } else {
                        (entry.key.to_owned(), entry.value.to_owned())
                    }
                })
                .collect()
        };

        let dict = PyDict::new(py);
        for (key, value) in entries {
            dict.set_item(key, value.into_pyobject(py)?)?;
        }
        Ok(dict.into())
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        match self.get_all(py) {
            Ok(result) => Ok(format!("Config({result})")),
            Err(err) => Ok(format!("Error: {:?}", err.to_string())),
        }
    }

    pub fn add_extension(&self, extension: Bound<PyAny>) -> PyResult<()> {
        let capsule = extension.getattr("__datafusion_extension_options__")?;
        let capsule = capsule.downcast::<PyCapsule>().map_err(py_datafusion_err)?;

        validate_pycapsule(capsule, "datafusion_extension_options")?;

        let mut extension = unsafe { capsule.reference::<FFI_ExtensionOptions>() }.clone();

        let config_lock = self.config.read();
        if let Some(prior_extension) = config_lock.extensions.get::<FFI_ExtensionOptions>() {
            extension
                .merge(prior_extension)
                .map_err(py_datafusion_err)?;
        }

        let mut config_lock = self.config.write();
        config_lock.extensions.insert(extension);

        Ok(())
    }
}
