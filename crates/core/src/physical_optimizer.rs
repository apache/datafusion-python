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

//! Imports physical optimizer rules supplied by another library over FFI.
//!
//! DataFusion has no FFI bridge for the logical [`OptimizerRule`] /
//! [`AnalyzerRule`] traits, but it does export
//! [`FFI_PhysicalOptimizerRule`] for the physical
//! [`PhysicalOptimizerRule`] trait. A producer crate (typically a separate
//! compiled extension) exposes an object with a
//! ``__datafusion_physical_optimizer_rule__`` method returning a
//! :class:`PyCapsule` that wraps an [`FFI_PhysicalOptimizerRule`]. This
//! module reads that capsule and converts it into an
//! ``Arc<dyn PhysicalOptimizerRule>`` so it can be registered with a
//! [`SessionContext`](datafusion::prelude::SessionContext) at construction
//! time.
//!
//! [`OptimizerRule`]: datafusion::optimizer::optimizer::OptimizerRule
//! [`AnalyzerRule`]: datafusion::optimizer::analyzer::AnalyzerRule

use std::ptr::NonNull;
use std::sync::Arc;

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_ffi::physical_optimizer::FFI_PhysicalOptimizerRule;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::errors::{PyDataFusionError, PyDataFusionResult, to_datafusion_err};

/// Convert a Python object exposing ``__datafusion_physical_optimizer_rule__``
/// into an ``Arc<dyn PhysicalOptimizerRule>`` by reading its FFI capsule.
pub(crate) fn physical_optimizer_rule_from_pyobject(
    obj: &Bound<'_, PyAny>,
) -> PyDataFusionResult<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    if !obj.hasattr("__datafusion_physical_optimizer_rule__")? {
        return Err(PyDataFusionError::Common(
            "Expected physical optimizer rule object to define \
             __datafusion_physical_optimizer_rule__()"
                .to_string(),
        ));
    }

    let capsule = obj
        .getattr("__datafusion_physical_optimizer_rule__")?
        .call0()?;
    let capsule = capsule.cast::<PyCapsule>().map_err(to_datafusion_err)?;
    let data: NonNull<FFI_PhysicalOptimizerRule> = capsule
        .pointer_checked(Some(c"datafusion_physical_optimizer_rule"))?
        .cast();
    let ffi_rule = unsafe { data.as_ref() };

    Ok(Arc::<dyn PhysicalOptimizerRule + Send + Sync>::from(
        ffi_rule,
    ))
}
