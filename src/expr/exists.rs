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

use datafusion::logical_expr::expr::Exists;
use pyo3::prelude::*;

use super::subquery::PySubquery;

#[pyclass(name = "Exists", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyExists {
    exists: Exists,
}

impl From<Exists> for PyExists {
    fn from(exists: Exists) -> Self {
        PyExists { exists }
    }
}

#[pymethods]
impl PyExists {
    fn subquery(&self) -> PySubquery {
        self.exists.subquery.clone().into()
    }

    fn negated(&self) -> bool {
        self.exists.negated
    }
}
