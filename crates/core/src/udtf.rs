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

use std::ptr::NonNull;
use std::sync::Arc;

use datafusion::catalog::{Session, TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::Expr;
use datafusion_ffi::udtf::FFI_TableFunction;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::{PyImportError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict, PyTuple, PyType};

use crate::context::PySessionContext;
use crate::errors::{py_datafusion_err, to_datafusion_err};
use crate::expr::PyExpr;
use crate::table::PyTable;

/// A pure-Python UDTF callable plus the metadata we discovered about it
/// at registration time.
#[derive(Debug, Clone)]
pub(crate) struct PythonTableFunctionCallable {
    pub(crate) callable: Arc<Py<PyAny>>,
    /// When true, the calling :class:`SessionContext` is passed to the
    /// callable as a ``session`` keyword argument on every invocation.
    /// Opt-in at registration time via ``with_session=True`` on the
    /// Python wrapper.
    pub(crate) inject_session_on_call: bool,
}

/// Represents a user defined table function
#[pyclass(from_py_object, frozen, name = "TableFunction", module = "datafusion")]
#[derive(Debug, Clone)]
pub struct PyTableFunction {
    pub(crate) name: String,
    pub(crate) inner: PyTableFunctionInner,
}

#[derive(Debug, Clone)]
pub(crate) enum PyTableFunctionInner {
    PythonFunction(PythonTableFunctionCallable),
    FFIFunction(Arc<dyn TableFunctionImpl>),
}

#[pymethods]
impl PyTableFunction {
    #[new]
    #[pyo3(signature=(name, func, session, inject_session_on_call=false))]
    pub fn new(
        name: &str,
        func: Bound<'_, PyAny>,
        session: Option<Bound<PyAny>>,
        inject_session_on_call: bool,
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
            let capsule = capsule.cast::<PyCapsule>()?;
            let data: NonNull<FFI_TableFunction> = capsule
                .pointer_checked(Some(c"datafusion_table_function"))?
                .cast();
            let ffi_func = unsafe { data.as_ref() };
            let foreign_func: Arc<dyn TableFunctionImpl> = ffi_func.to_owned().into();

            PyTableFunctionInner::FFIFunction(foreign_func)
        } else {
            PyTableFunctionInner::PythonFunction(PythonTableFunctionCallable {
                callable: Arc::new(func.unbind()),
                inject_session_on_call,
            })
        };

        Ok(Self {
            name: name.to_string(),
            inner,
        })
    }

    #[pyo3(signature = (*args))]
    pub fn __call__(&self, args: Vec<PyExpr>) -> PyResult<PyTable> {
        let args: Vec<Expr> = args.iter().map(|e| e.expr.clone()).collect();
        let global = PySessionContext::global_ctx()?;
        let state = global.ctx.state();
        let table_provider = self
            .call_with_args(TableFunctionArgs::new(&args, &state))
            .map_err(py_datafusion_err)?;

        Ok(PyTable::from(table_provider))
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("TableUDF({})", self.name))
    }
}

/// Materialize a fresh :class:`PySessionContext` from the borrowed
/// ``&dyn Session`` handed in at call time.
///
/// Upstream invokes ``call_with_args`` with a trait-object reference
/// rather than an owned context; we downcast it to the canonical
/// :class:`SessionState` impl and rebuild a :class:`SessionContext`
/// (sharing the same registries via the Arc-heavy interior of
/// :class:`SessionState`).
///
/// The downcast is defensive. Every path that reaches a pure-Python
/// UDTF today hands us a `SessionState`: the SQL planner builds the
/// args from its own `SessionState`, and `PyTableFunction::__call__`
/// uses the global context's state. A non-`SessionState` session
/// (e.g. a `ForeignSession`) would only arrive if this UDTF were
/// exported across the FFI boundary to a foreign-library consumer,
/// which datafusion-python does not do. Should that change, this
/// returns an error rather than silently misbehaving.
fn py_session_from_session(session: &dyn Session) -> DataFusionResult<PySessionContext> {
    let state = session
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "Cannot expose this UDTF's calling session to Python: the \
                 session is not a SessionState. Drop the `session` keyword \
                 from the callback signature to fall back to the \
                 expression-only call form."
                    .to_string(),
            )
        })?;
    Ok(PySessionContext::from(SessionContext::new_with_state(
        state.clone(),
    )))
}

#[allow(clippy::result_large_err)]
fn call_python_table_function(
    func: &PythonTableFunctionCallable,
    args: TableFunctionArgs,
) -> DataFusionResult<Arc<dyn TableProvider>> {
    let py_session = if func.inject_session_on_call {
        Some(py_session_from_session(args.session())?)
    } else {
        None
    };
    let py_exprs = args
        .exprs()
        .iter()
        .map(|arg| PyExpr::from(arg.clone()))
        .collect::<Vec<_>>();

    Python::attach(|py| {
        let py_args = PyTuple::new(py, py_exprs)?;
        let provider_obj = if let Some(session) = py_session {
            let kwargs = PyDict::new(py);
            kwargs.set_item("session", session.into_pyobject(py)?)?;
            func.callable.call(py, py_args, Some(&kwargs))?
        } else {
            func.callable.call1(py, py_args)?
        };
        let provider = provider_obj.bind(py).clone();

        Ok::<Arc<dyn TableProvider>, PyErr>(PyTable::new(provider, None)?.table)
    })
    .map_err(to_datafusion_err)
}

impl TableFunctionImpl for PyTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DataFusionResult<Arc<dyn TableProvider>> {
        match &self.inner {
            PyTableFunctionInner::FFIFunction(func) => func.call_with_args(args),
            PyTableFunctionInner::PythonFunction(callable) => {
                call_python_table_function(callable, args)
            }
        }
    }
}
