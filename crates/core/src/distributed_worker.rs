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

use std::collections::HashMap;
use std::net::SocketAddr;

use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion_distributed::{Worker, WorkerQueryContext, WorkerSessionBuilder};
use datafusion_python_util::wait_for_future;
use pyo3::Borrowed;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use tonic::transport::Server;

use crate::context::PySessionContext;
use crate::errors::{PyDataFusionError, PyDataFusionResult};

#[pyclass(
    from_py_object,
    frozen,
    name = "Worker",
    module = "datafusion",
    subclass
)]
#[derive(Clone)]
pub struct PyWorker {
    worker: Worker,
}

#[pymethods]
impl PyWorker {
    #[new]
    fn new() -> Self {
        Self {
            worker: Worker::default(),
        }
    }

    #[staticmethod]
    fn from_session_builder(session_builder: PyWorkerSessionBuilder) -> Self {
        Self {
            worker: Worker::from_session_builder(session_builder),
        }
    }

    fn with_version(&self, version: String) -> Self {
        Self {
            worker: self.worker.clone().with_version(version),
        }
    }

    fn with_max_message_size(&self, size: usize) -> Self {
        Self {
            worker: self.worker.clone().with_max_message_size(size),
        }
    }

    #[pyo3(signature = (host = "127.0.0.1", port = 50051))]
    fn serve(&self, py: Python<'_>, host: &str, port: u16) -> PyDataFusionResult<()> {
        let addr = parse_socket_addr(host, port)?;
        let worker = self.worker.clone();
        wait_for_future(py, serve_worker(worker, addr))?.map_err(PyDataFusionError::from)
    }

    #[pyo3(signature = (host = "127.0.0.1", port = 50051))]
    fn serve_async<'py>(
        &self,
        py: Python<'py>,
        host: &str,
        port: u16,
    ) -> PyResult<Bound<'py, PyAny>> {
        let addr = parse_socket_addr(host, port)?;
        let worker = self.worker.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            serve_worker(worker, addr)
                .await
                .map_err(PyDataFusionError::from)?;
            Ok(())
        })
    }
}

#[pyclass(name = "WorkerQueryContext", module = "datafusion", subclass)]
pub struct PyWorkerQueryContext {
    builder: Option<SessionStateBuilder>,
    headers: HashMap<String, String>,
}

impl PyWorkerQueryContext {
    fn new(ctx: WorkerQueryContext) -> Self {
        let headers = ctx
            .headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    value.to_str().unwrap_or_default().to_owned(),
                )
            })
            .collect();

        Self {
            builder: Some(ctx.builder),
            headers,
        }
    }
}

#[pymethods]
impl PyWorkerQueryContext {
    fn session_context(mut slf: PyRefMut<'_, Self>) -> PyResult<PySessionContext> {
        let builder = slf.builder.take().ok_or_else(|| {
            PyRuntimeError::new_err("WorkerQueryContext.session_context() can only be called once")
        })?;
        Ok(PySessionContext::from_session_state(builder.build()))
    }

    #[getter]
    fn headers(&self) -> HashMap<String, String> {
        self.headers.clone()
    }
}

pub(crate) struct PyWorkerSessionBuilder {
    callback: Py<PyAny>,
}

impl FromPyObject<'_, '_> for PyWorkerSessionBuilder {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, '_, PyAny>) -> Result<Self, Self::Error> {
        if !obj.is_callable() {
            return Err(PyTypeError::new_err(
                "Expected worker session builder to be callable",
            ));
        }

        Ok(Self {
            callback: obj.to_owned().unbind(),
        })
    }
}

#[async_trait]
impl WorkerSessionBuilder for PyWorkerSessionBuilder {
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError> {
        Python::attach(|py| -> PyResult<SessionState> {
            let ctx = Py::new(py, PyWorkerQueryContext::new(ctx))?;
            let result = self.callback.call1(py, (ctx,))?;
            let session_context = extract_session_context(result.bind(py))?;
            Ok(session_context.ctx.state())
        })
        .map_err(|error| DataFusionError::External(Box::new(error)))
    }
}

fn extract_session_context(obj: &Bound<'_, PyAny>) -> PyResult<PySessionContext> {
    if let Ok(session_context) = obj.extract::<PySessionContext>() {
        return Ok(session_context);
    }

    if let Ok(ctx_attr) = obj.getattr("ctx")
        && let Ok(session_context) = ctx_attr.extract::<PySessionContext>()
    {
        return Ok(session_context);
    }

    Err(PyTypeError::new_err(
        "WorkerSessionBuilder.build_session_state() must return a datafusion.SessionContext",
    ))
}

fn parse_socket_addr(host: &str, port: u16) -> PyDataFusionResult<SocketAddr> {
    format!("{host}:{port}").parse().map_err(|error| {
        PyDataFusionError::Common(format!(
            "invalid worker bind address {host}:{port}: {error}"
        ))
    })
}

async fn serve_worker(worker: Worker, addr: SocketAddr) -> DataFusionResult<()> {
    Server::builder()
        .add_service(worker.into_worker_server())
        .serve(addr)
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))
}
