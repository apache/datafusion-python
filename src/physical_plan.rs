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

use datafusion::execution::context::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::{displayable, ExecutionPlan, SendableRecordBatchStream};
use datafusion_common::Result;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;

use crate::errors::py_datafusion_err;
use crate::record_batch::PyRecordBatch;
use crate::utils::wait_for_future;
use pyo3::prelude::*;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

#[pyclass(name = "ExecutionPlan", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyExecutionPlan {
    pub plan: Arc<dyn ExecutionPlan>,
}

impl PyExecutionPlan {
    /// creates a new PyPhysicalPlan
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { plan }
    }
}

#[pymethods]
impl PyExecutionPlan {
    /// Get the inputs to this plan
    pub fn children(&self) -> Vec<PyExecutionPlan> {
        self.plan
            .children()
            .iter()
            .map(|p| p.to_owned().into())
            .collect()
    }

    pub fn display(&self) -> String {
        let d = displayable(self.plan.as_ref());
        format!("{}", d.one_line())
    }

    pub fn display_indent(&self) -> String {
        let d = displayable(self.plan.as_ref());
        format!("{}", d.indent())
    }

    pub fn execute(&self, part: usize, py: Python) -> PyResult<PyRecordBatchStream> {
        let ctx = Arc::new(TaskContext::new(
            "task_id".to_string(),
            "session_id".to_string(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            Arc::new(RuntimeEnv::default()),
        ));
        // create a Tokio runtime to run the async code
        let rt = Runtime::new().unwrap();
        let plan = self.plan.clone();
        let fut: JoinHandle<Result<SendableRecordBatchStream>> =
            rt.spawn(async move { plan.execute(part, ctx) });
        let stream = wait_for_future(py, fut).map_err(|e| py_datafusion_err(e))?;
        Ok(PyRecordBatchStream::new(stream?))
    }
}

impl From<PyExecutionPlan> for Arc<dyn ExecutionPlan> {
    fn from(plan: PyExecutionPlan) -> Arc<dyn ExecutionPlan> {
        plan.plan.clone()
    }
}

impl From<Arc<dyn ExecutionPlan>> for PyExecutionPlan {
    fn from(plan: Arc<dyn ExecutionPlan>) -> PyExecutionPlan {
        PyExecutionPlan { plan: plan.clone() }
    }
}

#[pyclass(name = "RecordBatchStream", module = "datafusion", subclass)]
pub struct PyRecordBatchStream {
    stream: SendableRecordBatchStream,
}

impl PyRecordBatchStream {
    fn new(stream: SendableRecordBatchStream) -> Self {
        Self { stream }
    }
}

#[pymethods]
impl PyRecordBatchStream {
    fn next(&mut self, py: Python) -> PyResult<PyRecordBatch> {
        let result = self.stream.next();
        let batch = wait_for_future(py, result).unwrap()?; //.map_err(DataFusionError::from)?;
        Ok(batch.into())
    }
}
