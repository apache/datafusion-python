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
    collections::HashMap,
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use datafusion::{common::file_options::file_type::FileType, logical_expr::dml::CopyTo};
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::sql::logical::PyLogicalPlan;

use super::logical_node::LogicalNode;

#[pyclass(name = "CopyTo", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyCopyTo {
    copy: CopyTo,
}

impl From<PyCopyTo> for CopyTo {
    fn from(copy: PyCopyTo) -> Self {
        copy.copy
    }
}

impl From<CopyTo> for PyCopyTo {
    fn from(copy: CopyTo) -> PyCopyTo {
        PyCopyTo { copy }
    }
}

impl Display for PyCopyTo {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "CopyTo: {:?}", self.copy.output_url)
    }
}

impl LogicalNode for PyCopyTo {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.copy.input).clone())]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}

#[pymethods]
impl PyCopyTo {
    #[new]
    pub fn new(
        input: PyLogicalPlan,
        output_url: String,
        partition_by: Vec<String>,
        file_type: PyFileType,
        options: HashMap<String, String>,
    ) -> Self {
        PyCopyTo {
            copy: CopyTo {
                input: input.plan(),
                output_url,
                partition_by,
                file_type: file_type.file_type,
                options,
            },
        }
    }

    fn input(&self) -> PyLogicalPlan {
        PyLogicalPlan::from((*self.copy.input).clone())
    }

    fn output_url(&self) -> String {
        self.copy.output_url.clone()
    }

    fn partition_by(&self) -> Vec<String> {
        self.copy.partition_by.clone()
    }

    fn file_type(&self) -> PyFileType {
        PyFileType {
            file_type: self.copy.file_type.clone(),
        }
    }

    fn options(&self) -> HashMap<String, String> {
        self.copy.options.clone()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("CopyTo({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("CopyTo".to_string())
    }
}

#[pyclass(name = "FileType", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyFileType {
    file_type: Arc<dyn FileType>,
}

impl Display for PyFileType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "FileType: {}", self.file_type)
    }
}

#[pymethods]
impl PyFileType {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("FileType({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("FileType".to_string())
    }
}
