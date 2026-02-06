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

use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::Expr;
use datafusion_ffi::udtf::FFI_TableFunction;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::{PyImportError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::context::PySessionContext;
use crate::errors::{py_datafusion_err, to_datafusion_err};
use crate::expr::PyExpr;
use crate::table::PyTable;
use crate::utils::validate_pycapsule;

/// Represents a user defined table function
#[pyclass(frozen, name = "TableFunction", module = "datafusion")]
#[derive(Debug, Clone)]
pub struct PyTableFunction {
    pub(crate) name: String,
    pub(crate) inner: PyTableFunctionInner,
}

// TODO: Implement pure python based user defined table functions
#[derive(Debug, Clone)]
pub(crate) enum PyTableFunctionInner {
    PythonFunction(Arc<Py<PyAny>>),
    FFIFunction(Arc<dyn TableFunctionImpl>),
}

#[pymethods]
impl PyTableFunction {
    #[new]
    #[pyo3(signature=(name, func, session))]
    pub fn new(
        name: &str,
        func: Bound<'_, PyAny>,
        session: Option<Bound<PyAny>>,
    ) -> PyResult<Self> {
        let inner = if func.hasattr("__datafusion_table_function__")? {
            let py = func.py();
            let session = match session {
                Some(session) => session,
                None => PySessionContext::global_ctx()?.into_bound_py_any(py)?,
            };
            let capsule = func
                .getattr("__datafusion_table_function__")?
                .call1((session,)).map_err(|err| {
                if err.get_type(py).is(PyType::new::<PyTypeError>(py)) {
                    PyImportError::new_err("Incompatible libraries. DataFusion 52.0.0 introduced an incompatible signature change for table functions. Either downgrade DataFusion or upgrade your function library.")
                } else {
                    err
                }
            })?;
            let capsule = capsule.downcast::<PyCapsule>().map_err(py_datafusion_err)?;
            validate_pycapsule(capsule, "datafusion_table_function")?;

            let ffi_func = unsafe { capsule.reference::<FFI_TableFunction>() };
            let foreign_func: Arc<dyn TableFunctionImpl> = ffi_func.to_owned().into();

            PyTableFunctionInner::FFIFunction(foreign_func)
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
    pub fn __call__(&self, args: Vec<PyExpr>) -> PyResult<PyTable> {
        let args: Vec<Expr> = args.iter().map(|e| e.expr.clone()).collect();
        let table_provider = self.call(&args).map_err(py_datafusion_err)?;

        Ok(PyTable::from(table_provider))
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("TableUDF({})", self.name))
    }
}

#[allow(clippy::result_large_err)]
fn call_python_table_function(
    func: &Arc<Py<PyAny>>,
    args: &[Expr],
) -> DataFusionResult<Arc<dyn TableProvider>> {
    let args = args
        .iter()
        .map(|arg| PyExpr::from(arg.clone()))
        .collect::<Vec<_>>();

    // move |args: &[ArrayRef]| -> Result<ArrayRef, DataFusionError> {
    Python::attach(|py| {
        let py_args = PyTuple::new(py, args)?;
        let provider_obj = func.call1(py, py_args)?;
        let provider = provider_obj.bind(py).clone();

        Ok::<Arc<dyn TableProvider>, PyErr>(PyTable::new(provider, None)?.table)
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
