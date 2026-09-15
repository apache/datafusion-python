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

use std::sync::{Arc, Mutex};

use pyo3::types::{PyAnyMethods, PyCapsule, PyDict, PyDictMethods};
use pyo3::{Bound, Py, PyAny, PyResult, Python, pyclass, pymethods};

use crate::aggregate_udf::MySumUDF;
use crate::physical_optimizer::MyPhysicalOptimizerRule;
use crate::scalar_udf::IsNullUDF;
use crate::table_function::MyTableFunction;
use crate::table_provider::MyTableProvider;
use crate::window_udf::MyRankUDF;

/// A bundle contributing this library's three functions in one install.
///
/// The shape a function library takes: no codecs and no planner, so the whole
/// of its installation is what it declares here. The three function getters
/// take no argument, so unlike a provider these need nothing from `ctx` and
/// the objects are handed over unresolved for the host to wrap.
#[pyclass(
    from_py_object,
    name = "MyFunctionExtension",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Debug, Clone, Default)]
pub(crate) struct MyFunctionExtension {}

#[pymethods]
impl MyFunctionExtension {
    #[new]
    fn new() -> Self {
        Self {}
    }

    /// `ctx` is unused: nothing declared here is bound to a session.
    fn __datafusion_session_components__<'py>(
        &self,
        py: Python<'py>,
        ctx: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let _ = ctx;

        let components = py
            .import("datafusion")?
            .getattr("SessionExtensionComponents")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("udfs", (Py::new(py, IsNullUDF::new())?,))?;
        kwargs.set_item("udafs", (Py::new(py, MySumUDF::new()?)?,))?;
        kwargs.set_item("udwfs", (Py::new(py, MyRankUDF::new()?)?,))?;
        components.call((), Some(&kwargs))
    }
}

/// A bundle contributing two physical optimizer rules.
///
/// Two, because that is what makes accumulation observable: rules never
/// collide the way function names do, so both of these install and both fire.
/// Each carries its own counter, which is how a test tells them apart.
#[pyclass(
    from_py_object,
    name = "MyRuleExtension",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Debug, Clone, Default)]
pub(crate) struct MyRuleExtension {
    first: MyPhysicalOptimizerRule,
    second: MyPhysicalOptimizerRule,
}

#[pymethods]
impl MyRuleExtension {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    /// How many times the first declared rule has run.
    fn first_calls(&self) -> usize {
        self.first.optimize_calls()
    }

    /// How many times the second declared rule has run.
    fn second_calls(&self) -> usize {
        self.second.optimize_calls()
    }

    /// `ctx` is unused: a rule getter takes no argument, so there is nothing
    /// session-scoped to bind.
    fn __datafusion_session_components__<'py>(
        &self,
        py: Python<'py>,
        ctx: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let _ = ctx;

        let components = py
            .import("datafusion")?
            .getattr("SessionExtensionComponents")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item(
            "physical_optimizer_rules",
            (
                Py::new(py, self.first.clone())?,
                Py::new(py, self.second.clone())?,
            ),
        )?;
        components.call((), Some(&kwargs))
    }
}

/// A table function that records the codec chain it was handed.
///
/// `__datafusion_table_function__` takes the session and pulls the host's
/// logical codec off it, so *which* session it is resolved against is
/// observable rather than a matter of taste. A bundle cannot wrap one itself:
/// the context its components hook receives has none of the call's codecs yet.
/// Recording the ids here is what lets a test assert the host resolved it
/// against the finished handle instead.
#[pyclass(
    from_py_object,
    name = "RecordingTableFunction",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Debug, Clone)]
pub(crate) struct RecordingTableFunction {
    seen: Arc<Mutex<Vec<String>>>,
    inner: MyTableFunction,
}

#[pymethods]
impl RecordingTableFunction {
    #[new]
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
            inner: MyTableFunction::new(),
        }
    }

    /// The logical codec ids the session carried when this was resolved.
    fn codec_ids_seen(&self) -> Vec<String> {
        self.seen.lock().map(|ids| ids.clone()).unwrap_or_default()
    }

    fn __datafusion_table_function__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let ids: Vec<String> = session
            .call_method0("logical_extension_codec_ids")?
            .extract()?;
        if let Ok(mut seen) = self.seen.lock() {
            *seen = ids;
        }
        self.inner.__datafusion_table_function__(py, session)
    }
}

/// A bundle contributing a table and a table function.
///
/// Both are `(name, value)` pairs, because neither carries a name of its own
/// the way a scalar function's capsule does.
#[pyclass(
    from_py_object,
    name = "MyDataExtension",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Debug, Clone)]
pub(crate) struct MyDataExtension {
    function: RecordingTableFunction,
}

#[pymethods]
impl MyDataExtension {
    #[new]
    fn new() -> Self {
        Self {
            function: RecordingTableFunction::new(),
        }
    }

    /// The codec ids the declared table function was resolved against.
    fn codec_ids_seen(&self) -> Vec<String> {
        self.function.codec_ids_seen()
    }

    fn __datafusion_session_components__<'py>(
        &self,
        py: Python<'py>,
        ctx: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let _ = ctx;

        let components = py
            .import("datafusion")?
            .getattr("SessionExtensionComponents")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item(
            "table_providers",
            ((
                "declared_table",
                Py::new(py, MyTableProvider::new(3, 2, 1))?,
            ),),
        )?;
        kwargs.set_item(
            "udtfs",
            (("declared_function", Py::new(py, self.function.clone())?),),
        )?;
        components.call((), Some(&kwargs))
    }
}
