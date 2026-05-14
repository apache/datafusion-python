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

use arrow_schema::{DataType, Field, Schema};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion_ffi::execution_plan::FFI_ExecutionPlan;
use datafusion_python_util::get_tokio_runtime;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

/// Minimal downstream `ExecutionPlan` producer for FFI integration
/// tests. Emits a single-column empty plan via `FFI_ExecutionPlan`,
/// letting Python tests verify that
/// `ExecutionPlan.from_pycapsule(my_plan)` consumes a capsule sourced
/// from a non-datafusion-python Rust crate.
#[pyclass(
    from_py_object,
    name = "MyExecutionPlan",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Clone)]
pub(crate) struct MyExecutionPlan;

#[pymethods]
impl MyExecutionPlan {
    #[new]
    fn new() -> Self {
        Self
    }

    fn __datafusion_execution_plan__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let plan = Arc::new(EmptyExec::new(schema));
        let runtime = get_tokio_runtime().handle().clone();
        let ffi = FFI_ExecutionPlan::new(plan, Some(runtime));

        let name = cr"datafusion_execution_plan".into();
        PyCapsule::new(py, ffi, Some(name))
    }
}
