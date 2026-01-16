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
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::udtf::FFI_TableFunction;
use pyo3::types::PyCapsule;
use pyo3::{pyclass, pymethods, Bound, PyAny, PyResult, Python};

use crate::table_provider::MyTableProvider;
use crate::utils::ffi_logical_codec_from_pycapsule;

#[pyclass(name = "MyTableFunction", module = "datafusion_ffi_example", subclass)]
#[derive(Debug, Clone)]
pub(crate) struct MyTableFunction {
    logical_codec: FFI_LogicalExtensionCodec,
}

#[pymethods]
impl MyTableFunction {
    #[new]
    fn new(session: &Bound<PyAny>) -> PyResult<Self> {
        let logical_codec = ffi_logical_codec_from_pycapsule(session)?;

        Ok(Self { logical_codec })
    }

    fn __datafusion_table_function__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_table_function".into();

        let func = self.clone();
        let provider =
            FFI_TableFunction::new_with_ffi_codec(Arc::new(func), None, self.logical_codec.clone());

        PyCapsule::new(py, provider, Some(name))
    }
}

impl TableFunctionImpl for MyTableFunction {
    fn call(&self, _args: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let provider = MyTableProvider::new_from_ffi_session(self.logical_codec.clone(), 4, 3, 2)
            .create_table()?;
        Ok(Arc::new(provider))
    }
}
