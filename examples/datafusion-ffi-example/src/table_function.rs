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

use datafusion_catalog::{TableFunctionImpl, TableProvider};
use datafusion_common::error::Result as DataFusionResult;
use datafusion_expr::Expr;
use datafusion_ffi::udtf::FFI_TableFunction;
use pyo3::types::PyCapsule;
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};

use crate::table_provider::MyTableProvider;
use crate::utils::ffi_logical_codec_from_pycapsule;

#[pyclass(name = "MyTableFunction", module = "datafusion_ffi_example", subclass)]
#[derive(Debug, Clone)]
pub(crate) struct MyTableFunction {}

#[pymethods]
impl MyTableFunction {
    #[new]
    fn new() -> Self {
        Self {}
    }

    fn __datafusion_table_function__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_table_function".into();

        let func = self.clone();
        let codec = ffi_logical_codec_from_pycapsule(session)?;
        let provider = FFI_TableFunction::new_with_ffi_codec(Arc::new(func), None, codec);

        PyCapsule::new(py, provider, Some(name))
    }
}

impl TableFunctionImpl for MyTableFunction {
    fn call(&self, _args: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let provider = MyTableProvider::new(4, 3, 2).create_table()?;
        Ok(Arc::new(provider))
    }
}
