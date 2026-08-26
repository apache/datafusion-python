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

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use datafusion::common::DataFusionError;
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
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_python_util::{ffi_query_planner_from_pycapsule, get_tokio_runtime};
use datafusion_session::{QueryPlanner, Session};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::config::MyPlannerConfig;

#[derive(Default)]
struct PlannerObservations {
    plan_calls: AtomicUsize,
    last_max_rows: AtomicUsize,
    foreign_session: AtomicBool,
    foreign_provider: AtomicBool,
    foreign_plan: AtomicBool,
    used_fallback: AtomicBool,
}

impl fmt::Debug for PlannerObservations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlannerObservations")
            .field("plan_calls", &self.plan_calls)
            .field("last_max_rows", &self.last_max_rows)
            .finish_non_exhaustive()
    }
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

/// The row limit as the host spells it, where `MyPlannerConfig` is registered as
/// an ordinary config extension under its own `ConfigExtension::PREFIX`.
const MAX_ROWS_KEY: &str = "ffi_query_planner.max_rows";

/// The same setting as it appears once the session has crossed the FFI
/// boundary. Rebuilding a `ConfigOptions` on this side parks every foreign
/// extension inside a single `FFI_ExtensionOptions`, which is itself a config
/// extension namespaced under `datafusion_ffi`, so `ConfigOptions::entries`
/// reports the key with both prefixes.
const FFI_MAX_ROWS_KEY: &str = "datafusion_ffi.ffi_query_planner.max_rows";

fn planner_config(session: &dyn Session) -> datafusion::common::Result<MyPlannerConfig> {
    let options = session.config_options();

    // Prefer the raw entry. `local_or_ffi_extension` discards a value it cannot
    // parse and hands back `MyPlannerConfig::default()`, which would quietly turn
    // a typo into a different row limit instead of reporting it.
    let config = match options
        .entries()
        .into_iter()
        .find(|entry| entry.key == MAX_ROWS_KEY || entry.key == FFI_MAX_ROWS_KEY)
    {
        Some(entry) => {
            let value = entry.value.ok_or_else(|| {
                DataFusionError::Configuration(format!("{} must have a value", entry.key))
            })?;
            let max_rows = value.parse::<usize>().map_err(|err| {
                DataFusionError::Configuration(format!(
                    "Invalid value '{value}' for {}: {err}",
                    entry.key
                ))
            })?;
            MyPlannerConfig { max_rows }
        }
        None => options
            .local_or_ffi_extension::<MyPlannerConfig>()
            .unwrap_or_default(),
    };

    // Validate after both paths so the fallback cannot smuggle in a limit that
    // the direct path rejects.
    if config.max_rows == 0 {
        return Err(DataFusionError::Configuration(format!(
            "{MAX_ROWS_KEY} must be greater than zero"
        )));
    }

    Ok(config)
}

struct DistributedQueryPlanner {
    observations: Arc<PlannerObservations>,
    /// Keeps the context behind the exported codecs' `TaskContextProvider`
    /// alive. See [`MyQueryPlanner::codec_ctx`]. The reference lives here as
    /// well as on `MyQueryPlanner` because this is the value the capsule
    /// carries, so the provider stays valid even if the Python object that
    /// exported it is dropped first.
    #[expect(
        dead_code,
        reason = "strong reference keeping the FFI codecs' weakly held provider alive"
    )]
    codec_ctx: Arc<SessionContext>,
    /// Planner to hand the work to instead of planning here.
    ///
    /// This is how a real planner layers on top of an existing one. The capsule
    /// must be captured from the session *before* this planner is installed:
    /// `SessionContext.__datafusion_query_planner__` exports whatever planner
    /// is installed at the time it is called, so capturing it afterwards would
    /// hand this planner a handle to itself.
    ///
    /// Note that `Session::create_physical_plan` cannot be used for this. It
    /// dispatches through the session's installed query planner, so calling it
    /// from inside that planner recurses until the stack overflows.
    fallback: Option<Arc<dyn QueryPlanner + Send + Sync>>,
}

impl fmt::Debug for DistributedQueryPlanner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DistributedQueryPlanner")
            .field("observations", &self.observations)
            .field("has_fallback", &self.fallback.is_some())
            .finish_non_exhaustive()
    }
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

        let plan = match self.fallback.as_ref() {
            Some(fallback) => {
                self.observations
                    .used_fallback
                    .store(true, Ordering::SeqCst);
                fallback.create_physical_plan(logical_plan, session).await?
            }
            None => {
                DefaultPhysicalPlanner::default()
                    .create_physical_plan(logical_plan, session)
                    .await?
            }
        };
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
#[derive(Clone)]
pub(crate) struct MyQueryPlanner {
    observations: Arc<PlannerObservations>,
    fallback: Option<Arc<dyn QueryPlanner + Send + Sync>>,
    /// Context backing the `TaskContextProvider` handed to the exported
    /// codecs.
    ///
    /// This is *not* the session that arrives at `create_physical_plan`. That
    /// one belongs to the host and is what this planner reads config from. The
    /// provider here serves the other direction: when the host decodes the
    /// plan bytes this library returned, any extension node in them is decoded
    /// by a callback back into this library, and that callback needs a
    /// `TaskContext` whose registry can resolve *this* library's nodes and
    /// functions. A real planner library registers its UDFs and extension
    /// types on this context; the example uses the default codecs, so nothing
    /// ever calls back.
    ///
    /// It must be owned rather than built inline: `FFI_TaskContextProvider`
    /// downgrades the provider to a `Weak`, so a temporary would already be
    /// dropped by the time the capsule is used, and every codec callback would
    /// fail with "TaskContextProvider went out of scope over FFI boundary".
    codec_ctx: Arc<SessionContext>,
}

impl Default for MyQueryPlanner {
    fn default() -> Self {
        Self {
            observations: Arc::default(),
            fallback: None,
            codec_ctx: Arc::new(SessionContext::new()),
        }
    }
}

impl fmt::Debug for MyQueryPlanner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MyQueryPlanner")
            .field("observations", &self.observations)
            .field("has_fallback", &self.fallback.is_some())
            .finish_non_exhaustive()
    }
}

#[pymethods]
impl MyQueryPlanner {
    /// Build a planner, optionally layered on top of an existing one.
    ///
    /// `fallback` takes anything exporting `__datafusion_query_planner__`,
    /// including a `SessionContext`. Capture it *before* installing this
    /// planner on that context, or the capsule will describe this planner and
    /// planning will recurse.
    #[new]
    #[pyo3(signature = (fallback=None))]
    fn new(fallback: Option<Bound<'_, PyAny>>) -> PyResult<Self> {
        let fallback = fallback
            .map(|planner| {
                ffi_query_planner_from_pycapsule(&planner)
                    .map(|ffi| -> Arc<dyn QueryPlanner + Send + Sync> { (&ffi).into() })
            })
            .transpose()?;
        Ok(Self {
            fallback,
            ..Self::default()
        })
    }

    fn used_fallback(&self) -> bool {
        self.observations.used_fallback.load(Ordering::SeqCst)
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
            codec_ctx: Arc::clone(&self.codec_ctx),
            fallback: self.fallback.clone(),
        });
        let runtime = get_tokio_runtime().handle().clone();
        let ctx_provider = Arc::clone(&self.codec_ctx) as Arc<dyn TaskContextProvider>;
        let ffi = FFI_QueryPlanner::new(
            planner,
            Some(runtime),
            &ctx_provider,
            Arc::new(DefaultLogicalExtensionCodec {}),
            Arc::new(DefaultPhysicalExtensionCodec {}),
        );
        PyCapsule::new_with_value(py, ffi, cr"datafusion_query_planner")
    }
}
