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

use std::fmt::{self, Display, Formatter};

use datafusion::common::Constraints;
use pyo3::prelude::*;

#[pyclass(name = "Constraints", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyConstraints {
    pub constraints: Constraints,
}

impl From<PyConstraints> for Constraints {
    fn from(constraints: PyConstraints) -> Self {
        constraints.constraints
    }
}

impl From<Constraints> for PyConstraints {
    fn from(constraints: Constraints) -> Self {
        PyConstraints { constraints }
    }
}

impl Display for PyConstraints {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Constraints: {:?}", self.constraints)
    }
}
