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
use datafusion_python_util::{
    ffi_logical_codec_from_pycapsule, ffi_physical_codec_from_pycapsule,
    ffi_query_planner_from_pycapsule,
};
use datafusion_session::{QueryPlanner, Session};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::config::MyPlannerConfig;

/// What the planner saw, accumulated across every call rather than reset each
/// time.
///
/// Two kinds of field here, and mixing them up is easy. `last_max_rows`
/// reports the most recent value, as its name says. Everything else is
/// cumulative: a count, or a "did this ever happen" flag written with
/// `fetch_or` so a later plan cannot retract an earlier observation. Tests
/// assert after running more than one query, so a flag that only described the
/// most recent plan would be answering a different question than the one its
/// accessor name asks.
#[derive(Default)]
struct PlannerObservations {
    plan_calls: AtomicUsize,
    last_max_rows: AtomicUsize,
    foreign_session: AtomicBool,
    foreign_provider: AtomicBool,
    foreign_plan: AtomicBool,
    /// Only ever set to `true`, so it is already cumulative.
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

#[derive(Debug)]
struct DistributedQueryPlanner {
    observations: Arc<PlannerObservations>,
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

#[async_trait]
impl QueryPlanner for DistributedQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.observations.plan_calls.fetch_add(1, Ordering::SeqCst);
        // `fetch_or`, not `store`: these answer "was this ever seen", so a
        // later plan that happens not to touch a foreign object must not
        // retract what an earlier one observed. A bare `SELECT 1` after a
        // scan of a foreign provider would otherwise clear the flag.
        self.observations
            .foreign_session
            .fetch_or(session.as_any().is::<ForeignSession>(), Ordering::SeqCst);
        self.observations.foreign_provider.fetch_or(
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
            .fetch_or(physical_plan_has_foreign_plan(&plan), Ordering::SeqCst);

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
    /// Held as the Python object rather than an imported planner, and resolved
    /// in `__datafusion_query_planner__` where a session is in hand.
    ///
    /// Importing it here would mean calling its getter with no session, which
    /// only a `SessionContext` or a raw capsule accepts. Another foreign
    /// planner -- the case that matters, since layering is the whole point of
    /// a fallback -- implements the same protocol this type does and requires
    /// the argument.
    fallback: Option<Arc<Py<PyAny>>>,
}

#[pymethods]
impl MyQueryPlanner {
    /// Build a planner, optionally layered on top of an existing one.
    ///
    /// `fallback` takes anything exporting `__datafusion_query_planner__`:
    /// another planner library, a `SessionContext`, or a raw capsule. It is
    /// imported when this planner is installed, not here, so that the session
    /// can be handed to its getter.
    ///
    /// Passing a `SessionContext` delegates to whichever planner that context
    /// holds at install time. If you instead capture a capsule with
    /// `ctx.__datafusion_query_planner__()`, capture it *before* installing
    /// this planner on that context, or the capsule will describe this planner
    /// and planning will recurse.
    #[new]
    #[pyo3(signature = (fallback=None))]
    fn new(fallback: Option<Bound<'_, PyAny>>) -> Self {
        Self {
            fallback: fallback.map(|obj| Arc::new(obj.unbind())),
            ..Self::default()
        }
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

    /// Export the planner, bound to the session it is being installed on.
    ///
    /// The codecs come off `session` rather than being built here. They carry
    /// the host's `TaskContextProvider`, so this library never constructs a
    /// `SessionContext`, and `with_query_planner` would rebind them to the
    /// running session anyway.
    fn __datafusion_query_planner__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        // Resolved here rather than in `new` so the fallback's own getter
        // receives the session, which is what the protocol requires of every
        // implementation other than a `SessionContext`.
        let fallback = self
            .fallback
            .as_ref()
            .map(|planner| {
                ffi_query_planner_from_pycapsule(planner.bind(py), Some(&session))
                    .map(|ffi| -> Arc<dyn QueryPlanner + Send + Sync> { (&ffi).into() })
            })
            .transpose()?;

        let planner: Arc<dyn QueryPlanner + Send + Sync> = Arc::new(DistributedQueryPlanner {
            observations: Arc::clone(&self.observations),
            fallback,
        });
        let logical_codec = ffi_logical_codec_from_pycapsule(session.clone(), None)?;
        let physical_codec = ffi_physical_codec_from_pycapsule(session, None)?;
        let ffi = FFI_QueryPlanner::new_with_ffi_codecs(planner, logical_codec, physical_codec);
        PyCapsule::new_with_value(py, ffi, cr"datafusion_query_planner")
    }
}
