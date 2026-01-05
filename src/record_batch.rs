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

use datafusion::arrow::pyarrow::ToPyArrow;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use pyo3::exceptions::{PyStopAsyncIteration, PyStopIteration};
use pyo3::prelude::*;
use pyo3::{pyclass, pymethods, PyAny, PyResult, Python};
use tokio::sync::Mutex;

use crate::errors::PyDataFusionError;
use crate::utils::wait_for_future;

#[pyclass(name = "RecordBatch", module = "datafusion", subclass, frozen)]
pub struct PyRecordBatch {
    batch: RecordBatch,
}

#[pymethods]
impl PyRecordBatch {
    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.batch.to_pyarrow(py)
    }
}

impl From<RecordBatch> for PyRecordBatch {
    fn from(batch: RecordBatch) -> Self {
        Self { batch }
    }
}

#[pyclass(name = "RecordBatchStream", module = "datafusion", subclass, frozen)]
pub struct PyRecordBatchStream {
    stream: Arc<Mutex<SendableRecordBatchStream>>,
}

impl PyRecordBatchStream {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        Self {
            stream: Arc::new(Mutex::new(stream)),
        }
    }
}

#[pymethods]
impl PyRecordBatchStream {
    fn next(&self, py: Python) -> PyResult<PyRecordBatch> {
        let stream = self.stream.clone();
        wait_for_future(py, next_stream(stream, true))?
    }

    fn __next__(&self, py: Python) -> PyResult<PyRecordBatch> {
        self.next(py)
    }

    fn __anext__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, next_stream(stream, false))
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
}

/// Polls the next batch from a `SendableRecordBatchStream`, converting the `Option<Result<_>>` form.
pub(crate) async fn poll_next_batch(
    stream: &mut SendableRecordBatchStream,
) -> datafusion::error::Result<Option<RecordBatch>> {
    stream.next().await.transpose()
}

async fn next_stream(
    stream: Arc<Mutex<SendableRecordBatchStream>>,
    sync: bool,
) -> PyResult<PyRecordBatch> {
    let mut stream = stream.lock().await;
    match poll_next_batch(&mut stream).await {
        Ok(Some(batch)) => Ok(batch.into()),
        Ok(None) => {
            // Depending on whether the iteration is sync or not, we raise either a
            // StopIteration or a StopAsyncIteration
            if sync {
                Err(PyStopIteration::new_err("stream exhausted"))
            } else {
                Err(PyStopAsyncIteration::new_err("stream exhausted"))
            }
        }
        Err(e) => Err(PyDataFusionError::from(e))?,
    }
}
