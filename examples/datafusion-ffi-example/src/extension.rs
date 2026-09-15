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

use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};
use pyo3::{Bound, Py, PyAny, PyResult, Python, pyclass, pymethods};

use crate::aggregate_udf::MySumUDF;
use crate::physical_optimizer::MyPhysicalOptimizerRule;
use crate::scalar_udf::IsNullUDF;
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
/// Each carries its own counter, which is how a test tells them apart, and
/// both append to one run log, which is how a test sees the order they
/// installed in.
#[pyclass(
    from_py_object,
    name = "MyRuleExtension",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Debug, Clone)]
pub(crate) struct MyRuleExtension {
    first: MyPhysicalOptimizerRule,
    second: MyPhysicalOptimizerRule,
    run_log: Arc<Mutex<Vec<usize>>>,
}

#[pymethods]
impl MyRuleExtension {
    #[new]
    fn new() -> Self {
        let run_log = Arc::new(Mutex::new(Vec::new()));
        Self {
            first: MyPhysicalOptimizerRule::with_run_log(0, Arc::clone(&run_log)),
            second: MyPhysicalOptimizerRule::with_run_log(1, Arc::clone(&run_log)),
            run_log,
        }
    }

    /// How many times the first declared rule has run.
    fn first_calls(&self) -> usize {
        self.first.optimize_calls()
    }

    /// How many times the second declared rule has run.
    fn second_calls(&self) -> usize {
        self.second.optimize_calls()
    }

    /// The labels of the two declared rules, in the order they ran: `0` is the
    /// first declared and `1` the second.
    fn run_order(&self) -> Vec<usize> {
        self.run_log.lock().expect("run log poisoned").clone()
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
