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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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
///
/// A rule declared alongside siblings also appends its label to a log they
/// all share, which is what lets a test see the order they ran in. Counters
/// alone cannot: each rule has its own, so they say how often but not when.
#[derive(Debug)]
struct CountingPhysicalOptimizerRule {
    optimize_calls: Arc<AtomicUsize>,
    label: usize,
    run_log: Option<Arc<Mutex<Vec<usize>>>>,
}

impl PhysicalOptimizerRule for CountingPhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(run_log) = &self.run_log {
            run_log.lock().expect("run log poisoned").push(self.label);
        }
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
    label: usize,
    run_log: Option<Arc<Mutex<Vec<usize>>>>,
}

impl MyPhysicalOptimizerRule {
    /// A rule that records where it ran relative to the siblings sharing
    /// `run_log`. `label` is what it appends. Not exposed to Python: only a
    /// bundle declaring several rules at once has siblings to order against.
    pub(crate) fn with_run_log(label: usize, run_log: Arc<Mutex<Vec<usize>>>) -> Self {
        Self {
            optimize_calls: Arc::new(AtomicUsize::new(0)),
            label,
            run_log: Some(run_log),
        }
    }
}

#[pymethods]
impl MyPhysicalOptimizerRule {
    #[new]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn optimize_calls(&self) -> usize {
        self.optimize_calls.load(Ordering::SeqCst)
    }

    fn __datafusion_physical_optimizer_rule__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> =
            Arc::new(CountingPhysicalOptimizerRule {
                optimize_calls: Arc::clone(&self.optimize_calls),
                label: self.label,
                run_log: self.run_log.clone(),
            });

        let runtime = get_tokio_runtime().handle().clone();
        let ffi = FFI_PhysicalOptimizerRule::new(rule, Some(runtime));

        PyCapsule::new_with_value(py, ffi, cr"datafusion_physical_optimizer_rule")
    }
}
