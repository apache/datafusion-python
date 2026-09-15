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

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::aggregate_udf::MySumUDF;
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
