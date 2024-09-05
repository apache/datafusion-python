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

use datafusion::common::DFSchema;
use pyo3::prelude::*;

#[derive(Debug, Clone)]
#[pyclass(name = "DFSchema", module = "datafusion.common", subclass)]
pub struct PyDFSchema {
    schema: Arc<DFSchema>,
}

impl From<PyDFSchema> for DFSchema {
    fn from(schema: PyDFSchema) -> DFSchema {
        (*schema.schema).clone()
    }
}

impl From<DFSchema> for PyDFSchema {
    fn from(schema: DFSchema) -> PyDFSchema {
        PyDFSchema {
            schema: Arc::new(schema),
        }
    }
}

#[pymethods]
impl PyDFSchema {
    #[pyo3(name = "empty")]
    #[staticmethod]
    fn py_empty() -> PyResult<Self> {
        Ok(Self {
            schema: Arc::new(DFSchema::empty()),
        })
    }

    #[pyo3(name = "field_names")]
    fn py_field_names(&self) -> PyResult<Vec<String>> {
        Ok(self.schema.field_names())
    }
}
