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

use std::any::Any;

use datafusion_common::config::{
    ConfigEntry, ConfigExtension, ConfigField, ExtensionOptions, Visit,
};
use datafusion_common::{DataFusionError, config_err};
use datafusion_ffi::config::extension_options::FFI_ExtensionOptions;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

#[pyclass(
    from_py_object,
    name = "PlannerConfig",
    module = "datafusion_ffi_query_planner_example",
    subclass
)]
#[derive(Clone, Debug)]
pub(crate) struct PlannerConfig {
    pub max_rows: usize,
}

#[pymethods]
impl PlannerConfig {
    #[new]
    #[pyo3(signature = (max_rows=10))]
    fn new(max_rows: usize) -> Self {
        Self { max_rows }
    }

    fn __datafusion_extension_options__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let mut config = FFI_ExtensionOptions::default();
        config
            .add_config(self)
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        PyCapsule::new_with_value(py, config, cr"datafusion_extension_options")
    }
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self { max_rows: 10 }
    }
}

impl ConfigExtension for PlannerConfig {
    const PREFIX: &'static str = "ffi_query_planner";
}

impl ExtensionOptions for PlannerConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> datafusion_common::Result<()> {
        ConfigField::set(self, key, value)
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![ConfigEntry {
            key: "max_rows".to_owned(),
            value: Some(self.max_rows.to_string()),
            description: "Maximum rows returned by the example query planner",
        }]
    }
}

impl ConfigField for PlannerConfig {
    fn visit<V: Visit>(&self, visitor: &mut V, _key: &str, _description: &'static str) {
        self.max_rows.visit(
            visitor,
            "max_rows",
            "Maximum rows returned by the example query planner",
        );
    }

    fn set(&mut self, key: &str, value: &str) -> Result<(), DataFusionError> {
        let (key, rem) = key.split_once('.').unwrap_or((key, ""));
        match key {
            "max_rows" => self.max_rows.set(rem, value),
            _ => config_err!("Config value '{key}' not found on PlannerConfig"),
        }
    }
}
