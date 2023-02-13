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

use datafusion::arrow::datatypes::Field;
use pyo3::prelude::*;

use crate::common::data_type::DataTypeMap;

/// PyDFField wraps an arrow-datafusion `DFField` struct type
/// and also supplies convenience methods for interacting
/// with the `DFField` instance in the context of Python
#[pyclass(name = "DFField", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyDFField {
    /// Optional qualifier (usually a table or relation name)
    #[allow(dead_code)]
    qualifier: Option<String>,
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    data_type: DataTypeMap,
    /// Arrow field definition
    #[allow(dead_code)]
    field: Field,
    #[allow(dead_code)]
    index: usize,
}
