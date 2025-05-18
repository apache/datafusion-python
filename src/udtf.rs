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

use pyo3::prelude::*;
use std::sync::Arc;

use crate::dataframe::PyTableProvider;
use crate::errors::{py_datafusion_err, to_datafusion_err};
use crate::expr::PyExpr;
use crate::utils::validate_pycapsule;
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::Expr;
use datafusion_ffi::table_provider::{FFI_TableProvider, ForeignTableProvider};
use datafusion_ffi::udtf::{FFI_TableFunction, ForeignTableFunction};
use pyo3::exceptions::PyNotImplementedError;
use pyo3::types::{PyCapsule, PyTuple};

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
    PythonFunction(Arc<PyObject>),
    FFIFunction(Arc<dyn TableFunctionImpl>),
}

#[pymethods]
impl PyTableFunction {
    #[new]
    #[pyo3(signature=(name, func))]
    pub fn new(name: &str, func: Bound<'_, PyAny>) -> PyResult<Self> {
        let inner = if func.hasattr("__datafusion_table_function__")? {
            let capsule = func.getattr("__datafusion_table_function__")?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>().map_err(py_datafusion_err)?;
            validate_pycapsule(capsule, "datafusion_table_function")?;

            let ffi_func = unsafe { capsule.reference::<FFI_TableFunction>() };
            let foreign_func: ForeignTableFunction = ffi_func.to_owned().into();

            PyTableFunctionInner::FFIFunction(Arc::new(foreign_func))
        } else {
            let py_obj = Arc::new(func.unbind());
            PyTableFunctionInner::PythonFunction(py_obj)
        };

        Ok(Self {
            name: name.to_string(),
            inner,
        })
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

#[allow(clippy::result_large_err)]
fn call_python_table_function(
    func: &Arc<PyObject>,
    args: &[Expr],
) -> DataFusionResult<Arc<dyn TableProvider>> {
    let args = args
        .iter()
        .map(|arg| PyExpr::from(arg.clone()))
        .collect::<Vec<_>>();

    // move |args: &[ArrayRef]| -> Result<ArrayRef, DataFusionError> {
    Python::with_gil(|py| {
        let py_args = PyTuple::new(py, args)?;
        let provider_obj = func.call1(py, py_args)?;
        let provider = provider_obj.bind(py);

        if provider.hasattr("__datafusion_table_provider__")? {
            let capsule = provider.getattr("__datafusion_table_provider__")?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>().map_err(py_datafusion_err)?;
            validate_pycapsule(capsule, "datafusion_table_provider")?;

            let provider = unsafe { capsule.reference::<FFI_TableProvider>() };
            let provider: ForeignTableProvider = provider.into();

            Ok(Arc::new(provider) as Arc<dyn TableProvider>)
        } else {
            Err(PyNotImplementedError::new_err(
                "__datafusion_table_provider__ does not exist on Table Provider object.",
            ))
        }
    })
    .map_err(to_datafusion_err)
}

impl TableFunctionImpl for PyTableFunction {
    fn call(&self, args: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        match &self.inner {
            PyTableFunctionInner::FFIFunction(func) => func.call(args),
            PyTableFunctionInner::PythonFunction(obj) => call_python_table_function(obj, args),
        }
    }
}
