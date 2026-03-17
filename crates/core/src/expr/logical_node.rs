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

use pyo3::{Bound, PyAny, PyResult, Python};

use crate::sql::logical::PyLogicalPlan;

/// Representation of a `LogicalNode` in the in overall `LogicalPlan`
/// any "node" shares these common traits in common.
pub trait LogicalNode {
    /// The input plan to the current logical node instance.
    fn inputs(&self) -> Vec<PyLogicalPlan>;

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>>;
}
