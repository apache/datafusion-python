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

//! Reference implementation of a downstream-crate `ExecutionPlan`
//! exported to datafusion-python via the PyCapsule protocol.
//!
//! Typical use case: a Rust crate produces a custom physical plan
//! (e.g. for a remote backend, a streaming source, a synthetic data
//! generator) and wants to hand it to a datafusion-python user
//! without exposing its internals. The downstream crate writes a
//! `#[pyclass]` wrapper, builds an `Arc<dyn ExecutionPlan>` on
//! demand, wraps it as `FFI_ExecutionPlan`, and exposes the capsule
//! via `__datafusion_execution_plan__(...)`. The receiving Python
//! code calls
//! `datafusion.ExecutionPlan.from_pycapsule(my_plan)` and gets back
//! a fully-typed datafusion-python `ExecutionPlan` it can introspect,
//! serialize via `to_bytes` / `from_bytes`, or execute.
//!
//! `MyExecutionPlan` here is a minimal worked example: it produces
//! `num_rows` sequential `Int32` values in a single batch under the
//! column name `value`. Backed by `MemorySourceConfig`, so the plan
//! is a real `DataSourceExec` with a non-trivial schema and
//! executable output — useful both as a reference for downstream
//! crates and as the fixture for the FFI integration tests.

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::error::{DataFusionError, Result as DataFusionResult};
use datafusion_ffi::execution_plan::FFI_ExecutionPlan;
use datafusion_python_util::get_tokio_runtime;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

/// Downstream-crate `ExecutionPlan` producer for FFI integration with
/// datafusion-python. Emits `num_rows` sequential `Int32` values
/// (0..num_rows) in a single batch under the column name `value`.
#[pyclass(
    from_py_object,
    name = "MyExecutionPlan",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Clone)]
pub(crate) struct MyExecutionPlan {
    num_rows: usize,
}

impl MyExecutionPlan {
    fn build_plan(&self) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let values: Vec<i32> = (0..self.num_rows as i32).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(values))],
        )?;
        let exec = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
        Ok(exec as Arc<dyn ExecutionPlan>)
    }
}

#[pymethods]
impl MyExecutionPlan {
    #[new]
    fn new(num_rows: usize) -> Self {
        Self { num_rows }
    }

    fn __datafusion_execution_plan__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let plan = self
            .build_plan()
            .map_err(|e: DataFusionError| PyRuntimeError::new_err(e.to_string()))?;
        let runtime = get_tokio_runtime().handle().clone();
        let ffi = FFI_ExecutionPlan::new(plan, Some(runtime));

        let name = cr"datafusion_execution_plan".into();
        PyCapsule::new(py, ffi, Some(name))
    }
}
