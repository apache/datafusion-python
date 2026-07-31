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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use datafusion::execution::TaskContextProvider;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_catalog::default_table_source::source_as_provider;
use datafusion_ffi::config::ExtensionOptionsFFIProvider;
use datafusion_ffi::execution_plan::ForeignExecutionPlan;
use datafusion_ffi::query_planner::FFI_QueryPlanner;
use datafusion_ffi::session::ForeignSession;
use datafusion_ffi::table_provider::ForeignTableProvider;
use datafusion_python_util::get_tokio_runtime;
use datafusion_session::{QueryPlanner, Session};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::config::PlannerConfig;

#[derive(Debug, Default)]
struct PlannerObservations {
    plan_calls: AtomicUsize,
    last_max_rows: AtomicUsize,
    foreign_session: AtomicBool,
    foreign_provider: AtomicBool,
    foreign_plan: AtomicBool,
}

fn logical_plan_has_foreign_provider(plan: &LogicalPlan) -> bool {
    if let LogicalPlan::TableScan(scan) = plan
        && let Ok(provider) = source_as_provider(&scan.source)
        && provider.downcast_ref::<ForeignTableProvider>().is_some()
    {
        return true;
    }
    plan.inputs()
        .iter()
        .any(|input| logical_plan_has_foreign_provider(input))
}

fn physical_plan_has_foreign_plan(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<ForeignExecutionPlan>()
        || plan
            .children()
            .iter()
            .any(|child| physical_plan_has_foreign_plan(child))
}

fn planner_config(session: &dyn Session) -> datafusion::common::Result<PlannerConfig> {
    let options = session.config_options();

    // Read the flattened entry first. Some DataFusion revisions add an extra
    // `datafusion_ffi` namespace while reconstructing a ForeignSession. Parsing
    // it directly also ensures malformed values are reported instead of being
    // replaced silently by PlannerConfig::default().
    if let Some(entry) = options
        .entries()
        .into_iter()
        .find(|entry| entry.key.ends_with("ffi_query_planner.max_rows"))
    {
        let value = entry.value.ok_or_else(|| {
            datafusion::common::DataFusionError::Configuration(format!(
                "{} must have a value",
                entry.key
            ))
        })?;
        let max_rows = value.parse::<usize>().map_err(|err| {
            datafusion::common::DataFusionError::Configuration(format!(
                "Invalid value '{value}' for {}: {err}",
                entry.key
            ))
        })?;
        if max_rows == 0 {
            return Err(datafusion::common::DataFusionError::Configuration(
                "ffi_query_planner.max_rows must be greater than zero".to_owned(),
            ));
        }
        return Ok(PlannerConfig { max_rows });
    }

    Ok(options
        .local_or_ffi_extension::<PlannerConfig>()
        .unwrap_or_default())
}

#[derive(Debug)]
struct DistributedQueryPlanner {
    observations: Arc<PlannerObservations>,
}

#[async_trait]
impl QueryPlanner for DistributedQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.observations.plan_calls.fetch_add(1, Ordering::SeqCst);
        self.observations
            .foreign_session
            .store(session.as_any().is::<ForeignSession>(), Ordering::SeqCst);
        self.observations.foreign_provider.store(
            logical_plan_has_foreign_provider(logical_plan),
            Ordering::SeqCst,
        );

        let config = planner_config(session)?;
        self.observations
            .last_max_rows
            .store(config.max_rows, Ordering::SeqCst);

        let plan = DefaultPhysicalPlanner::default()
            .create_physical_plan(logical_plan, session)
            .await?;
        self.observations
            .foreign_plan
            .store(physical_plan_has_foreign_plan(&plan), Ordering::SeqCst);

        Ok(Arc::new(GlobalLimitExec::new(
            plan,
            0,
            Some(config.max_rows),
        )))
    }
}

#[pyclass(
    from_py_object,
    name = "MyQueryPlanner",
    module = "datafusion_ffi_query_planner_example",
    subclass
)]
#[derive(Debug, Default, Clone)]
pub(crate) struct MyQueryPlanner {
    observations: Arc<PlannerObservations>,
}

#[pymethods]
impl MyQueryPlanner {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn plan_calls(&self) -> usize {
        self.observations.plan_calls.load(Ordering::SeqCst)
    }

    fn last_max_rows(&self) -> usize {
        self.observations.last_max_rows.load(Ordering::SeqCst)
    }

    fn foreign_session_observed(&self) -> bool {
        self.observations.foreign_session.load(Ordering::SeqCst)
    }

    fn foreign_provider_observed(&self) -> bool {
        self.observations.foreign_provider.load(Ordering::SeqCst)
    }

    fn foreign_plan_observed(&self) -> bool {
        self.observations.foreign_plan.load(Ordering::SeqCst)
    }

    fn __datafusion_query_planner__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let planner: Arc<dyn QueryPlanner + Send + Sync> = Arc::new(DistributedQueryPlanner {
            observations: Arc::clone(&self.observations),
        });
        let runtime = get_tokio_runtime().handle().clone();
        let ctx_provider = Arc::new(SessionContext::new()) as Arc<dyn TaskContextProvider>;
        let ffi = FFI_QueryPlanner::new(planner, Some(runtime), &ctx_provider, None, None);
        PyCapsule::new_with_value(py, ffi, cr"datafusion_query_planner")
    }
}
