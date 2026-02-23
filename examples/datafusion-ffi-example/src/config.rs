use std::any::Any;

use datafusion_common::config::{
    ConfigEntry, ConfigExtension, ConfigField, ExtensionOptions, Visit,
};
use datafusion_common::{DataFusionError, config_err};
use datafusion_ffi::config::extension_options::FFI_ExtensionOptions;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyCapsule;
use pyo3::{Bound, PyResult, Python, pyclass, pymethods};

/// My own config options.
#[pyclass(name = "MyConfig", module = "datafusion_ffi_example", subclass)]
#[derive(Clone, Debug)]
pub struct MyConfig {
    /// Should "foo" be replaced by "bar"?
    pub foo_to_bar: bool,

    /// How many "baz" should be created?
    pub baz_count: usize,
}

#[pymethods]
impl MyConfig {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn __datafusion_extension_options__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_extension_options".into();

        let mut config = FFI_ExtensionOptions::default();
        config
            .add_config(self)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        PyCapsule::new(py, config, Some(name))
    }
}

impl Default for MyConfig {
    fn default() -> Self {
        Self {
            foo_to_bar: true,
            baz_count: 1337,
        }
    }
}

impl ConfigExtension for MyConfig {
    const PREFIX: &'static str = "my_config";
}

impl ExtensionOptions for MyConfig {
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
        datafusion_common::config::ConfigField::set(self, key, value)
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![
            ConfigEntry {
                key: "foo_to_bar".to_owned(),
                value: Some(format!("{}", self.foo_to_bar)),
                description: "foo to bar",
            },
            ConfigEntry {
                key: "baz_count".to_owned(),
                value: Some(format!("{}", self.baz_count)),
                description: "baz count",
            },
        ]
    }
}

impl ConfigField for MyConfig {
    fn visit<V: Visit>(&self, v: &mut V, _key: &str, _description: &'static str) {
        let key = "foo_to_bar";
        let desc = "foo to bar";
        self.foo_to_bar.visit(v, key, desc);

        let key = "baz_count";
        let desc = "baz count";
        self.baz_count.visit(v, key, desc);
    }

    fn set(&mut self, key: &str, value: &str) -> Result<(), DataFusionError> {
        let (key, rem) = key.split_once('.').unwrap_or((key, ""));
        match key {
            "foo_to_bar" => self.foo_to_bar.set(rem, value.as_ref()),
            "baz_count" => self.baz_count.set(rem, value.as_ref()),

            _ => config_err!("Config value \"{}\" not found on MyConfig", key),
        }
    }
}
