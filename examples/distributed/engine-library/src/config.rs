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

//! This engine's session config.
//!
//! A config extension rather than a constructor argument, because the driver
//! and every worker have to agree on the shuffle directory and the config is
//! the one thing that travels with the session. It also has to be *registered*
//! before anything can set `dfx_engine.shuffle_dir`: an unknown namespace is
//! an error, not a no-op, which is the first thing a worker bootstrap gets
//! wrong.

use std::any::Any;

use datafusion_common::config::{
    ConfigEntry, ConfigExtension, ConfigField, ExtensionOptions, Visit,
};
use datafusion_common::{DataFusionError, config_err};
use datafusion_ffi::config::extension_options::FFI_ExtensionOptions;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

/// Options under the `dfx_engine` prefix.
#[pyclass(from_py_object, name = "DfxEngineConfig", module = "dfx_engine")]
#[derive(Clone, Debug, Default)]
pub(crate) struct DfxEngineConfig {
    /// Directory stage results are exchanged through. Empty means "do not
    /// distribute": the planner leaves the plan alone and it runs in process.
    pub(crate) shuffle_dir: String,
}

#[pymethods]
impl DfxEngineConfig {
    #[new]
    #[pyo3(signature = (shuffle_dir=String::new()))]
    fn new(shuffle_dir: String) -> Self {
        Self { shuffle_dir }
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

impl ConfigExtension for DfxEngineConfig {
    const PREFIX: &'static str = "dfx_engine";
}

impl ExtensionOptions for DfxEngineConfig {
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
            key: "shuffle_dir".to_owned(),
            value: Some(self.shuffle_dir.clone()),
            description: "directory stage results are exchanged through",
        }]
    }
}

impl ConfigField for DfxEngineConfig {
    fn visit<V: Visit>(&self, v: &mut V, _key: &str, _description: &'static str) {
        self.shuffle_dir.visit(
            v,
            "shuffle_dir",
            "directory stage results are exchanged through",
        );
    }

    fn set(&mut self, key: &str, value: &str) -> Result<(), DataFusionError> {
        let (key, rem) = key.split_once('.').unwrap_or((key, ""));
        match key {
            "shuffle_dir" => self.shuffle_dir.set(rem, value),
            _ => config_err!("Config value \"{key}\" not found on DfxEngineConfig"),
        }
    }
}
