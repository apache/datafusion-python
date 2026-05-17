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

//! Bridges between user-provided Python rule classes and the upstream
//! [`OptimizerRule`] / [`AnalyzerRule`] traits.
//!
//! The Python side defines abstract base classes ``OptimizerRule`` and
//! ``AnalyzerRule`` with ``name()`` plus, respectively, ``rewrite(plan)``
//! and ``analyze(plan)``. Instances are wrapped in
//! [`PyOptimizerRuleAdapter`] / [`PyAnalyzerRuleAdapter`] before being
//! handed to [`SessionContext::add_optimizer_rule`] /
//! [`SessionContext::add_analyzer_rule`].
//!
//! `rewrite` may return ``None`` to signal "no transformation" — the
//! adapter maps that to [`Transformed::no`]. Any returned
//! :class:`LogicalPlan` becomes [`Transformed::yes`]. `analyze` is
//! mandatory-rewrite (must return a plan); returning ``None`` is an
//! error.
//!
//! The upstream ``&dyn OptimizerConfig`` / ``&ConfigOptions`` arguments
//! are not surfaced to Python in this MVP. Rules that need configuration
//! access should be implemented in Rust today; Python rules read state
//! from the plan and from any captured ``SessionContext`` they were
//! constructed with.

use std::fmt;
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::optimizer::{OptimizerConfig, OptimizerRule};
use pyo3::prelude::*;

use crate::errors::to_datafusion_err;
use crate::sql::logical::PyLogicalPlan;

/// Wraps a Python ``OptimizerRule`` instance so that it can be registered
/// with the upstream optimizer pipeline.
pub struct PyOptimizerRuleAdapter {
    rule: Py<PyAny>,
    name: String,
}

impl PyOptimizerRuleAdapter {
    pub fn new(rule: Bound<'_, PyAny>) -> PyResult<Self> {
        let name = rule.call_method0("name")?.extract::<String>()?;
        Ok(Self {
            rule: rule.unbind(),
            name,
        })
    }
}

impl fmt::Debug for PyOptimizerRuleAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PyOptimizerRuleAdapter")
            .field("name", &self.name)
            .finish()
    }
}

impl OptimizerRule for PyOptimizerRuleAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        Python::attach(|py| {
            let py_plan = PyLogicalPlan::from(plan.clone());
            let result = self
                .rule
                .bind(py)
                .call_method1("rewrite", (py_plan,))
                .map_err(to_datafusion_err)?;
            if result.is_none() {
                return Ok(Transformed::no(plan));
            }
            let rewritten: PyLogicalPlan = result.extract().map_err(to_datafusion_err)?;
            Ok(Transformed::yes(LogicalPlan::from(rewritten)))
        })
    }
}

/// Wraps a Python ``AnalyzerRule`` instance so that it can be registered
/// with the upstream analyzer pipeline.
pub struct PyAnalyzerRuleAdapter {
    rule: Py<PyAny>,
    name: String,
}

impl PyAnalyzerRuleAdapter {
    pub fn new(rule: Bound<'_, PyAny>) -> PyResult<Self> {
        let name = rule.call_method0("name")?.extract::<String>()?;
        Ok(Self {
            rule: rule.unbind(),
            name,
        })
    }
}

impl fmt::Debug for PyAnalyzerRuleAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PyAnalyzerRuleAdapter")
            .field("name", &self.name)
            .finish()
    }
}

impl AnalyzerRule for PyAnalyzerRuleAdapter {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DataFusionResult<LogicalPlan> {
        Python::attach(|py| {
            let py_plan = PyLogicalPlan::from(plan);
            let result = self
                .rule
                .bind(py)
                .call_method1("analyze", (py_plan,))
                .map_err(to_datafusion_err)?;
            if result.is_none() {
                return Err(DataFusionError::Execution(format!(
                    "AnalyzerRule {} returned None from analyze(); analyzer rules \
                     must return a LogicalPlan",
                    self.name
                )));
            }
            let rewritten: PyLogicalPlan = result.extract().map_err(to_datafusion_err)?;
            Ok(LogicalPlan::from(rewritten))
        })
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Construct an adapter from a Python ``OptimizerRule`` instance.
pub(crate) fn build_optimizer_rule(
    rule: Bound<'_, PyAny>,
) -> PyResult<Arc<dyn OptimizerRule + Send + Sync>> {
    Ok(Arc::new(PyOptimizerRuleAdapter::new(rule)?))
}

/// Construct an adapter from a Python ``AnalyzerRule`` instance.
pub(crate) fn build_analyzer_rule(
    rule: Bound<'_, PyAny>,
) -> PyResult<Arc<dyn AnalyzerRule + Send + Sync>> {
    Ok(Arc::new(PyAnalyzerRuleAdapter::new(rule)?))
}
