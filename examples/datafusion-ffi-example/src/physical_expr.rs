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

use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Literal;
use datafusion_common::ScalarValue;
use datafusion_ffi::physical_expr::FFI_PhysicalExpr;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

/// Minimal downstream `PhysicalExpr` producer for FFI integration
/// tests. Emits the literal `Int32(42)` via `FFI_PhysicalExpr` so
/// Python tests can verify `PhysicalExpr.from_pycapsule(my_expr)`
/// consumes a capsule sourced from a non-datafusion-python crate.
#[pyclass(
    from_py_object,
    name = "MyPhysicalExpr",
    module = "datafusion_ffi_example",
    subclass
)]
#[derive(Clone)]
pub(crate) struct MyPhysicalExpr;

#[pymethods]
impl MyPhysicalExpr {
    #[new]
    fn new() -> Self {
        Self
    }

    fn __datafusion_physical_expr__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(42))));
        let ffi = FFI_PhysicalExpr::from(expr);

        let name = cr"datafusion_physical_expr".into();
        PyCapsule::new(py, ffi, Some(name))
    }
}
