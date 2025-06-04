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

use pyo3::{prelude::*, types::PyBytes};

use crate::context::PySessionContext;
use crate::errors::{py_datafusion_err, PyDataFusionError, PyDataFusionResult};
use crate::sql::logical::PyLogicalPlan;
use crate::utils::wait_for_future;

use datafusion_substrait::logical_plan::{consumer, producer};
use datafusion_substrait::serializer;
use datafusion_substrait::substrait::proto::Plan;
use prost::Message;

#[pyclass(name = "Plan", module = "datafusion.substrait", subclass)]
#[derive(Debug, Clone)]
pub struct PyPlan {
    pub plan: Plan,
}

#[pymethods]
impl PyPlan {
    fn encode(&self, py: Python) -> PyResult<PyObject> {
        let mut proto_bytes = Vec::<u8>::new();
        self.plan
            .encode(&mut proto_bytes)
            .map_err(PyDataFusionError::EncodeError)?;
        Ok(PyBytes::new(py, &proto_bytes).into())
    }
}

impl From<PyPlan> for Plan {
    fn from(plan: PyPlan) -> Plan {
        plan.plan
    }
}

impl From<Plan> for PyPlan {
    fn from(plan: Plan) -> PyPlan {
        PyPlan { plan }
    }
}

/// A PySubstraitSerializer is a representation of a Serializer that is capable of both serializing
/// a `LogicalPlan` instance to Substrait Protobuf bytes and also deserialize Substrait Protobuf bytes
/// to a valid `LogicalPlan` instance.
#[pyclass(name = "Serde", module = "datafusion.substrait", subclass)]
#[derive(Debug, Clone)]
pub struct PySubstraitSerializer;

#[pymethods]
impl PySubstraitSerializer {
    #[staticmethod]
    pub fn serialize(
        sql: &str,
        ctx: PySessionContext,
        path: &str,
        py: Python,
    ) -> PyDataFusionResult<()> {
        // Clone the string values to create owned values that can be moved into the future
        let sql_owned = sql.to_string();
        let path_owned = path.to_string();
        let ctx_owned = ctx.ctx.clone();

        // Create a future that moves owned values
        let future =
            async move { serializer::serialize(&sql_owned, &ctx_owned, &path_owned).await };

        // Suppress the specific warning while maintaining the behavior that works with Tokio
        #[allow(unused_must_use)]
        wait_for_future(py, future)?.map_err(PyDataFusionError::from)?;

        Ok(())
    }

    #[staticmethod]
    pub fn serialize_to_plan(
        sql: &str,
        ctx: PySessionContext,
        py: Python,
    ) -> PyDataFusionResult<PyPlan> {
        // Clone sql string to own it
        let sql_owned = sql.to_string();

        PySubstraitSerializer::serialize_bytes(&sql_owned, ctx, py).and_then(|proto_bytes| {
            let proto_bytes = proto_bytes.bind(py).downcast::<PyBytes>().unwrap();
            PySubstraitSerializer::deserialize_bytes(proto_bytes.as_bytes().to_vec(), py)
        })
    }

    #[staticmethod]
    pub fn serialize_bytes(
        sql: &str,
        ctx: PySessionContext,
        py: Python,
    ) -> PyDataFusionResult<PyObject> {
        // Clone the string value to create an owned value that can be moved into the future
        let sql_owned = sql.to_string();
        let ctx_owned = ctx.ctx.clone();

        // Create a future that moves owned values
        let future = async move { serializer::serialize_bytes(&sql_owned, &ctx_owned).await };

        let proto_bytes: Vec<u8> = wait_for_future(py, future)??.into();
        Ok(PyBytes::new(py, &proto_bytes).into())
    }

    #[staticmethod]
    pub fn deserialize(path: &str, py: Python) -> PyDataFusionResult<PyPlan> {
        // Clone the path to own it
        let path_owned = path.to_string();

        // Create a future that moves owned values
        let future = async move { serializer::deserialize(&path_owned).await };

        let plan = wait_for_future(py, future)??;
        Ok(PyPlan { plan: *plan })
    }

    #[staticmethod]
    pub fn deserialize_bytes(proto_bytes: Vec<u8>, py: Python) -> PyDataFusionResult<PyPlan> {
        let plan = wait_for_future(py, serializer::deserialize_bytes(proto_bytes))??;
        Ok(PyPlan { plan: *plan })
    }
}

#[pyclass(name = "Producer", module = "datafusion.substrait", subclass)]
#[derive(Debug, Clone)]
pub struct PySubstraitProducer;

#[pymethods]
impl PySubstraitProducer {
    /// Convert DataFusion LogicalPlan to Substrait Plan
    #[staticmethod]
    pub fn to_substrait_plan(plan: PyLogicalPlan, ctx: &PySessionContext) -> PyResult<PyPlan> {
        // Clone the state to ensure it lives long enough
        let session_state = ctx.ctx.state().clone();
        let plan_ref = plan.plan.clone();

        match producer::to_substrait_plan(&plan_ref, &session_state) {
            Ok(plan) => Ok(PyPlan { plan: *plan }),
            Err(e) => Err(py_datafusion_err(e)),
        }
    }
}

#[pyclass(name = "Consumer", module = "datafusion.substrait", subclass)]
#[derive(Debug, Clone)]
pub struct PySubstraitConsumer;

#[pymethods]
impl PySubstraitConsumer {
    /// Convert Substrait Plan to DataFusion DataFrame
    #[staticmethod]
    pub fn from_substrait_plan(
        ctx: &mut PySessionContext,
        plan: PyPlan,
        py: Python,
    ) -> PyDataFusionResult<PyLogicalPlan> {
        // Clone the state to ensure it lives long enough
        let session_state = ctx.ctx.state().clone();
        let plan_clone = plan.plan.clone();

        // Create a future that takes ownership of the variables
        let future = async move {
            // The .await is needed here to actually execute the future
            consumer::from_substrait_plan(&session_state, &plan_clone).await
        };

        // Wait for the future and handle the result, using only a single ?
        let logical_plan = wait_for_future(py, future)?;

        // Handle the Result returned by the future
        match logical_plan {
            Ok(plan) => Ok(PyLogicalPlan::new(plan)),
            Err(e) => Err(PyDataFusionError::from(e)),
        }
    }
}

pub fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyPlan>()?;
    m.add_class::<PySubstraitConsumer>()?;
    m.add_class::<PySubstraitProducer>()?;
    m.add_class::<PySubstraitSerializer>()?;
    Ok(())
}
