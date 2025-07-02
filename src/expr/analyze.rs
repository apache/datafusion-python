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

use datafusion::logical_expr::logical_plan::Analyze;
use pyo3::{prelude::*, IntoPyObjectExt};
use std::fmt::{self, Display, Formatter};

use super::logical_node::LogicalNode;
use crate::common::df_schema::PyDFSchema;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "Analyze", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyAnalyze {
    analyze: Analyze,
}

impl PyAnalyze {
    pub fn new(analyze: Analyze) -> Self {
        Self { analyze }
    }
}

impl From<Analyze> for PyAnalyze {
    fn from(analyze: Analyze) -> PyAnalyze {
        PyAnalyze { analyze }
    }
}

impl From<PyAnalyze> for Analyze {
    fn from(analyze: PyAnalyze) -> Self {
        analyze.analyze
    }
}

impl Display for PyAnalyze {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Analyze Table")
    }
}

#[pymethods]
impl PyAnalyze {
    fn verbose(&self) -> PyResult<bool> {
        Ok(self.analyze.verbose)
    }

    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    /// Resulting Schema for this `Analyze` node instance
    fn schema(&self) -> PyResult<PyDFSchema> {
        Ok((*self.analyze.schema).clone().into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Analyze({self})"))
    }
}

impl LogicalNode for PyAnalyze {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.analyze.input).clone())]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
