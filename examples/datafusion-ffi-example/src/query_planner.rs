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

use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::execution::TaskContextProvider;
use datafusion::execution::context::{QueryPlanner, SessionContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion_ffi::query_planner::FFI_QueryPlanner;
use datafusion_python_util::get_tokio_runtime;
use datafusion_session::Session;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

#[derive(Debug)]
struct CountingQueryPlanner {
    plan_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl QueryPlanner for CountingQueryPlanner {
    async fn create_physical_plan(
        &self,
        _logical_plan: &LogicalPlan,
        _session: &dyn Session,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.plan_calls.fetch_add(1, Ordering::SeqCst);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        Ok(Arc::new(EmptyExec::new(schema)))
    }
}

/// Python-visible query planner used to test planning across a real FFI boundary.
#[pyclass(
    from_py_object,
    name = "MyQueryPlanner",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Debug, Default, Clone)]
pub(crate) struct MyQueryPlanner {
    plan_calls: Arc<AtomicUsize>,
}

#[pymethods]
impl MyQueryPlanner {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn plan_calls(&self) -> usize {
        self.plan_calls.load(Ordering::SeqCst)
    }

    fn __datafusion_query_planner__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let planner: Arc<dyn QueryPlanner + Send + Sync> = Arc::new(CountingQueryPlanner {
            plan_calls: Arc::clone(&self.plan_calls),
        });
        let runtime = get_tokio_runtime().handle().clone();
        let ctx_provider = Arc::new(SessionContext::new()) as Arc<dyn TaskContextProvider>;
        let ffi = FFI_QueryPlanner::new(planner, Some(runtime), &ctx_provider, None, None);

        PyCapsule::new_with_value(py, ffi, cr"datafusion_query_planner")
    }
}
