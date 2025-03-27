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

use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use datafusion::logical_expr::DropFunction;
use pyo3::{prelude::*, IntoPyObjectExt};

use super::logical_node::LogicalNode;
use crate::common::df_schema::PyDFSchema;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "DropFunction", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyDropFunction {
    drop: DropFunction,
}

impl From<PyDropFunction> for DropFunction {
    fn from(drop: PyDropFunction) -> Self {
        drop.drop
    }
}

impl From<DropFunction> for PyDropFunction {
    fn from(drop: DropFunction) -> PyDropFunction {
        PyDropFunction { drop }
    }
}

impl Display for PyDropFunction {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DropFunction")
    }
}

#[pymethods]
impl PyDropFunction {
    #[new]
    fn new(name: String, schema: PyDFSchema, if_exists: bool) -> PyResult<Self> {
        Ok(PyDropFunction {
            drop: DropFunction {
                name,
                schema: Arc::new(schema.into()),
                if_exists,
            },
        })
    }
    fn name(&self) -> PyResult<String> {
        Ok(self.drop.name.clone())
    }

    fn schema(&self) -> PyDFSchema {
        (*self.drop.schema).clone().into()
    }

    fn if_exists(&self) -> PyResult<bool> {
        Ok(self.drop.if_exists)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("DropFunction({})", self))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("DropFunction".to_string())
    }
}

impl LogicalNode for PyDropFunction {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
