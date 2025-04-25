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

use pyo3::prelude::*;

use crate::dataframe::PyTableProvider;
use crate::errors::py_datafusion_err;
use crate::expr::PyExpr;
use crate::utils::validate_pycapsule;
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::exec_err;
use datafusion::logical_expr::Expr;
use datafusion_ffi::udtf::{FFI_TableFunction, ForeignTableFunction};
use pyo3::types::PyCapsule;

/// Represents a user defined table function
#[pyclass(name = "TableFunction", module = "datafusion")]
#[derive(Debug, Clone)]
pub struct PyTableFunction {
    pub(crate) name: String,
    pub(crate) inner: PyTableFunctionInner,
}

// TODO: Implement pure python based user defined table functions
#[derive(Debug, Clone)]
pub(crate) enum PyTableFunctionInner {
    // PythonFunction(Arc<PyObject>),
    FFIFunction(Arc<dyn TableFunctionImpl>),
}

#[pymethods]
impl PyTableFunction {
    #[new]
    #[pyo3(signature=(name, func))]
    pub fn new(name: &str, func: Bound<'_, PyAny>) -> PyResult<Self> {
        if func.hasattr("__datafusion_table_function__")? {
            let capsule = func.getattr("__datafusion_table_function__")?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>().map_err(py_datafusion_err)?;
            validate_pycapsule(capsule, "datafusion_table_function")?;

            let ffi_func = unsafe { capsule.reference::<FFI_TableFunction>() };
            let foreign_func: ForeignTableFunction = ffi_func.to_owned().into();

            Ok(Self {
                name: name.to_string(),
                inner: PyTableFunctionInner::FFIFunction(Arc::new(foreign_func)),
            })
        } else {
            exec_err!("Python based Table Functions are not yet implemented")
                .map_err(py_datafusion_err)
        }
    }

    #[pyo3(signature = (*args))]
    pub fn __call__(&self, args: Vec<PyExpr>) -> PyResult<PyTableProvider> {
        let args: Vec<Expr> = args.iter().map(|e| e.expr.clone()).collect();
        let table_provider = self.call(&args).map_err(py_datafusion_err)?;

        Ok(PyTableProvider::new(table_provider))
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("TableUDF({})", self.name))
    }
}

impl TableFunctionImpl for PyTableFunction {
    fn call(&self, args: &[Expr]) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        match &self.inner {
            PyTableFunctionInner::FFIFunction(func) => func.call(args),
        }
    }
}
