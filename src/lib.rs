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

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
use pyo3::prelude::*;

// Re-export Apache Arrow DataFusion dependencies
pub use datafusion;
pub use datafusion::common as datafusion_common;
pub use datafusion::logical_expr as datafusion_expr;
pub use datafusion::optimizer;
pub use datafusion::sql as datafusion_sql;

#[cfg(feature = "substrait")]
pub use datafusion_substrait;

#[allow(clippy::borrow_deref_ref)]
pub mod catalog;
pub mod common;

#[allow(clippy::borrow_deref_ref)]
mod config;
#[allow(clippy::borrow_deref_ref)]
pub mod context;
#[allow(clippy::borrow_deref_ref)]
pub mod dataframe;
mod dataset;
mod dataset_exec;
pub mod errors;
#[allow(clippy::borrow_deref_ref)]
pub mod expr;
#[allow(clippy::borrow_deref_ref)]
mod functions;
pub mod physical_plan;
mod pyarrow_filter_expression;
mod record_batch;
pub mod sql;
pub mod store;

#[cfg(feature = "substrait")]
pub mod substrait;
#[allow(clippy::borrow_deref_ref)]
mod udaf;
#[allow(clippy::borrow_deref_ref)]
mod udf;
mod udwf;
pub mod utils;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Used to define Tokio Runtime as a Python module attribute
pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

/// Low-level DataFusion internal package.
///
/// The higher-level public API is defined in pure python files under the
/// datafusion directory.
#[pymodule]
fn _internal(py: Python, m: Bound<'_, PyModule>) -> PyResult<()> {
    // Register the python classes
    m.add_class::<catalog::PyCatalog>()?;
    m.add_class::<catalog::PyDatabase>()?;
    m.add_class::<catalog::PyTable>()?;
    m.add_class::<context::PyRuntimeConfig>()?;
    m.add_class::<context::PySessionConfig>()?;
    m.add_class::<context::PySessionContext>()?;
    m.add_class::<context::PySQLOptions>()?;
    m.add_class::<dataframe::PyDataFrame>()?;
    m.add_class::<udf::PyScalarUDF>()?;
    m.add_class::<udaf::PyAggregateUDF>()?;
    m.add_class::<udwf::PyWindowUDF>()?;
    m.add_class::<config::PyConfig>()?;
    m.add_class::<sql::logical::PyLogicalPlan>()?;
    m.add_class::<physical_plan::PyExecutionPlan>()?;
    m.add_class::<record_batch::PyRecordBatch>()?;
    m.add_class::<record_batch::PyRecordBatchStream>()?;

    // Register `common` as a submodule. Matching `datafusion-common` https://docs.rs/datafusion-common/latest/datafusion_common/
    let common = PyModule::new_bound(py, "common")?;
    common::init_module(&common)?;
    m.add_submodule(&common)?;

    // Register `expr` as a submodule. Matching `datafusion-expr` https://docs.rs/datafusion-expr/latest/datafusion_expr/
    let expr = PyModule::new_bound(py, "expr")?;
    expr::init_module(&expr)?;
    m.add_submodule(&expr)?;

    // Register the functions as a submodule
    let funcs = PyModule::new_bound(py, "functions")?;
    functions::init_module(&funcs)?;
    m.add_submodule(&funcs)?;

    let store = PyModule::new_bound(py, "object_store")?;
    store::init_module(&store)?;
    m.add_submodule(&store)?;

    // Register substrait as a submodule
    #[cfg(feature = "substrait")]
    setup_substrait_module(py, &m)?;

    Ok(())
}

#[cfg(feature = "substrait")]
fn setup_substrait_module(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let substrait = PyModule::new_bound(py, "substrait")?;
    substrait::init_module(&substrait)?;
    m.add_submodule(&substrait)?;
    Ok(())
}
