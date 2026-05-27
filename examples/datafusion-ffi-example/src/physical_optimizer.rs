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
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ffi::physical_optimizer::FFI_PhysicalOptimizerRule;
use datafusion_python_util::get_tokio_runtime;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

/// A physical optimizer rule that leaves every plan unchanged but bumps a
/// shared counter each time it runs. Tests use the counter to prove that a
/// session built with this rule actually routed physical planning through a
/// user-supplied [`PhysicalOptimizerRule`] over FFI.
#[derive(Debug)]
struct CountingPhysicalOptimizerRule {
    optimize_calls: Arc<AtomicUsize>,
}

impl PhysicalOptimizerRule for CountingPhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize_calls.fetch_add(1, Ordering::SeqCst);
        Ok(plan)
    }

    fn name(&self) -> &str {
        "counting_physical_optimizer_rule"
    }

    fn schema_check(&self) -> bool {
        // The plan is returned unchanged, so the schema is preserved.
        true
    }
}

/// Python-visible handle that produces an [`FFI_PhysicalOptimizerRule`] and
/// exposes the shared call counter.
#[pyclass(
    from_py_object,
    name = "MyPhysicalOptimizerRule",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Debug, Default, Clone)]
pub(crate) struct MyPhysicalOptimizerRule {
    optimize_calls: Arc<AtomicUsize>,
}

#[pymethods]
impl MyPhysicalOptimizerRule {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn optimize_calls(&self) -> usize {
        self.optimize_calls.load(Ordering::SeqCst)
    }

    fn __datafusion_physical_optimizer_rule__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> =
            Arc::new(CountingPhysicalOptimizerRule {
                optimize_calls: Arc::clone(&self.optimize_calls),
            });

        let runtime = get_tokio_runtime().handle().clone();
        let ffi = FFI_PhysicalOptimizerRule::new(rule, Some(runtime));

        let name = cr"datafusion_physical_optimizer_rule".into();
        PyCapsule::new(py, ffi, Some(name))
    }
}
