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

use crate::context::PySessionContext;
use crate::errors::{py_datafusion_err, DataFusionError};
use crate::sql::logical::PyLogicalPlan;
use crate::utils::wait_for_future;

use datafusion_substrait::logical_plan::{consumer, producer};
use datafusion_substrait::serializer;
use datafusion_substrait::substrait::proto::Plan;

#[pyclass(name = "plan", module = "datafusion.substrait", subclass, unsendable)]
#[derive(Debug, Clone)]
pub(crate) struct PyPlan {
    pub(crate) plan: Plan,
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
#[pyclass(name = "serde", module = "datafusion.substrait", subclass, unsendable)]
#[derive(Debug, Clone)]
pub(crate) struct PySubstraitSerializer;

#[pymethods]
impl PySubstraitSerializer {
    #[staticmethod]
    pub fn serialize(sql: &str, ctx: PySessionContext, path: &str, py: Python) -> PyResult<()> {
        wait_for_future(py, serializer::serialize(sql, &ctx.ctx, path))
            .map_err(DataFusionError::from)?;
        Ok(())
    }

    #[staticmethod]
    pub fn serialize_to_plan(sql: &str, ctx: PySessionContext, py: Python) -> PyResult<PyPlan> {
        match PySubstraitSerializer::serialize_bytes(sql, ctx, py) {
            Ok(proto_bytes) => PySubstraitSerializer::deserialize_bytes(proto_bytes, py),
            Err(e) => Err(py_datafusion_err(e)),
        }
    }

    #[staticmethod]
    pub fn serialize_bytes(sql: &str, ctx: PySessionContext, py: Python) -> PyResult<Vec<u8>> {
        let proto_bytes: Vec<u8> = wait_for_future(py, serializer::serialize_bytes(sql, &ctx.ctx))
            .map_err(DataFusionError::from)?;
        Ok(proto_bytes)
    }

    #[staticmethod]
    pub fn deserialize(path: &str, py: Python) -> PyResult<PyPlan> {
        let plan =
            wait_for_future(py, serializer::deserialize(path)).map_err(DataFusionError::from)?;
        Ok(PyPlan { plan: *plan })
    }

    #[staticmethod]
    pub fn deserialize_bytes(proto_bytes: Vec<u8>, py: Python) -> PyResult<PyPlan> {
        let plan = wait_for_future(py, serializer::deserialize_bytes(proto_bytes))
            .map_err(DataFusionError::from)?;
        Ok(PyPlan { plan: *plan })
    }
}

#[pyclass(
    name = "producer",
    module = "datafusion.substrait",
    subclass,
    unsendable
)]
#[derive(Debug, Clone)]
pub(crate) struct PySubstraitProducer;

#[pymethods]
impl PySubstraitProducer {
    /// Convert DataFusion LogicalPlan to Substrait Plan
    #[staticmethod]
    pub fn to_substrait_plan(plan: PyLogicalPlan) -> PyResult<PyPlan> {
        match producer::to_substrait_plan(&plan.plan) {
            Ok(plan) => Ok(PyPlan { plan: *plan }),
            Err(e) => Err(py_datafusion_err(e)),
        }
    }
}

#[pyclass(
    name = "consumer",
    module = "datafusion.substrait",
    subclass,
    unsendable
)]
#[derive(Debug, Clone)]
pub(crate) struct PySubstraitConsumer;

#[pymethods]
impl PySubstraitConsumer {
    /// Convert Substrait Plan to DataFusion DataFrame
    #[staticmethod]
    pub fn from_substrait_plan(
        ctx: &mut PySessionContext,
        plan: PyPlan,
        py: Python,
    ) -> PyResult<PyLogicalPlan> {
        let result = consumer::from_substrait_plan(&mut ctx.ctx, &plan.plan);
        let logical_plan = wait_for_future(py, result).map_err(DataFusionError::from)?;
        Ok(PyLogicalPlan::new(logical_plan))
    }
}

pub(crate) fn init_module(m: &PyModule) -> PyResult<()> {
    m.add_class::<PySubstraitConsumer>()?;
    m.add_class::<PySubstraitProducer>()?;
    m.add_class::<PySubstraitSerializer>()?;
    Ok(())
}
