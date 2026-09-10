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

//! A toy distributed engine, in the two halves a real one has.
//!
//! The Rust half is here: a query planner that splits the plan into stages,
//! the node that marks a stage, the codec that carries it, and a config
//! extension so the driver and its workers agree on where results go.
//!
//! The Python half is in `python/dfx_engine`: the session factory both sides
//! build from, the worker entry point, and the driver that fans work out. An
//! engine needs both, which is why this crate is a mixed maturin package
//! rather than a pure extension module.
//!
//! One of three libraries in `examples/distributed`. This one owns execution.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::config::DfxEngineConfig;
use crate::extension::{BundledPhysicalCodec, DfxEngineExtension};

mod codec;
mod config;
mod extension;
mod local_session;
mod planner;
mod stage;

/// Where the results of one stage partition live.
///
/// Exported so the Python worker writes the path the Rust node will read,
/// rather than the convention being spelled out on both sides of the
/// boundary and drifting.
#[pyfunction]
fn partition_path(shuffle_dir: &str, stage_id: u32, partition: usize) -> String {
    stage::partition_path(shuffle_dir, stage_id, partition)
        .to_string_lossy()
        .into_owned()
}

/// Id of the `index`th stage in a plan, counting in pre-order from the root.
///
/// Exported for the same reason as [`partition_path`]: a foreign node's
/// one-line display is replaced by the FFI wrapper's, so Python cannot read a
/// stage id back off a plan and has to agree with Rust about the numbering
/// instead. Keeping the arithmetic here means the two cannot drift.
#[pyfunction]
#[pyo3(signature = (index=0))]
fn stage_id(index: usize) -> PyResult<u32> {
    u32::try_from(index)
        .ok()
        .and_then(|index| planner::FIRST_STAGE_ID.checked_add(index))
        .ok_or_else(|| PyValueError::new_err(format!("stage index {index} is out of range")))
}

#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<BundledPhysicalCodec>()?;
    m.add_class::<DfxEngineConfig>()?;
    m.add_class::<DfxEngineExtension>()?;
    m.add_function(wrap_pyfunction!(partition_path, m)?)?;
    m.add_function(wrap_pyfunction!(stage_id, m)?)?;
    Ok(())
}
