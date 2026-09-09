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

//! A `Session` that borrows another one but owns its optimizer rules.
//!
//! Physical planning applies `session.physical_optimizers()`. When the session
//! arrived over FFI those rules are the *host's*, so each one runs back across
//! the boundary and hands this library a `ForeignExecutionPlan`. A stock
//! `CooperativeExec` produced that way has no reachable `try_to_proto`, so a
//! planner that must serialize its result -- and `FFI_QueryPlanner` always
//! must, it returns proto bytes rather than a handle -- fails on a node that
//! is perfectly serializable in the process that made it. See the "Known gaps"
//! section of the extension guide.
//!
//! Wrapping the session with a locally-owned copy of the same rule set keeps
//! every rewrite inside this library, where the nodes stay concrete. That is
//! also what lets this engine split the plan: it cannot rewrite a subtree it
//! is only holding an opaque handle to.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::CatalogProviderList;
use datafusion::common::config::{ConfigOptions, TableOptions};
use datafusion::common::{DFSchema, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::registry::ExtensionTypeRegistryRef;
use datafusion::logical_expr::{
    AggregateUDF, Expr, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_session::Session;

/// Borrows `inner` for everything except the physical optimizer rules.
pub(crate) struct LocalOptimizerSession<'a> {
    inner: &'a dyn Session,
    rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
}

impl<'a> LocalOptimizerSession<'a> {
    /// Wrap `inner` with the stock DataFusion rule set, owned here.
    pub(crate) fn new(inner: &'a dyn Session) -> Self {
        Self {
            inner,
            rules: datafusion::physical_optimizer::optimizer::PhysicalOptimizer::default().rules,
        }
    }
}

#[async_trait::async_trait]
impl Session for LocalOptimizerSession<'_> {
    /// The one override. Everything below delegates.
    fn physical_optimizers(&self) -> &[Arc<dyn PhysicalOptimizerRule + Send + Sync>] {
        &self.rules
    }

    fn session_id(&self) -> &str {
        self.inner.session_id()
    }

    fn config(&self) -> &SessionConfig {
        self.inner.config()
    }

    fn catalog_list(&self) -> Arc<dyn CatalogProviderList> {
        self.inner.catalog_list()
    }

    fn config_options(&self) -> &ConfigOptions {
        self.inner.config_options()
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.inner.optimize(plan)
    }

    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.create_physical_plan(logical_plan).await
    }

    fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.inner.create_physical_expr(expr, df_schema)
    }

    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        self.inner.scalar_functions()
    }

    fn higher_order_functions(&self) -> &HashMap<String, Arc<HigherOrderUDF>> {
        self.inner.higher_order_functions()
    }

    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        self.inner.aggregate_functions()
    }

    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        self.inner.window_functions()
    }

    fn extension_type_registry(&self) -> &ExtensionTypeRegistryRef {
        self.inner.extension_type_registry()
    }

    fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        self.inner.runtime_env()
    }

    fn execution_props(&self) -> &ExecutionProps {
        self.inner.execution_props()
    }

    fn as_any(&self) -> &dyn Any {
        // Delegated, not `self`: the return type is implicitly `&dyn Any +
        // 'static` and this wrapper only lives as long as its borrow. It also
        // keeps `as_any().is::<ForeignSession>()` answering about the real
        // session rather than the wrapper.
        self.inner.as_any()
    }

    fn table_options(&self) -> &TableOptions {
        self.inner.table_options()
    }

    fn table_options_mut(&mut self) -> &mut TableOptions {
        // The wrapper only borrows `inner`, so it cannot hand out a mutable
        // reference. Physical planning never calls this; verified in the spike.
        unimplemented!("LocalOptimizerSession does not support table_options_mut")
    }

    fn task_ctx(&self) -> Arc<TaskContext> {
        self.inner.task_ctx()
    }
}
