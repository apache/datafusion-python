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

//! Analyzer rules layered on top of DataFusion's defaults.

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::AnalyzerRule;

/// Resolve [`LambdaVariable`] references into bound lambda parameters.
///
/// DataFusion's SQL planner resolves lambda variables inline as it plans a
/// higher-order function call, so SQL-built plans never carry unresolved
/// variables. Plans assembled programmatically through the Python expression
/// builder (e.g. `array_transform(col("xs"), lambda_(["v"], lambda_var("v")))`)
/// do carry them, and nothing in the default analyzer resolves them. This rule
/// runs [`LogicalPlan::resolve_lambda_variables`] so both construction paths
/// reach the optimizer with bound lambdas.
///
/// [`LambdaVariable`]: datafusion::logical_expr::expr::LambdaVariable
#[derive(Debug)]
pub struct ResolveLambdaVariables {}

impl ResolveLambdaVariables {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for ResolveLambdaVariables {
    fn default() -> Self {
        Self::new()
    }
}

impl AnalyzerRule for ResolveLambdaVariables {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.resolve_lambda_variables().map(|t| t.data)
    }

    fn name(&self) -> &str {
        "resolve_lambda_variables"
    }
}
