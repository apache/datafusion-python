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

use std::collections::HashMap;

use datafusion::arrow::datatypes::DataType;
use pyo3::prelude::*;

use super::data_type::PyDataType;

#[pyclass(frozen, name = "SqlFunction", module = "datafusion.common", subclass)]
#[derive(Debug, Clone)]
pub struct SqlFunction {
    pub name: String,
    pub return_types: HashMap<Vec<DataType>, DataType>,
    pub aggregation: bool,
}

impl SqlFunction {
    pub fn new(
        function_name: String,
        input_types: Vec<PyDataType>,
        return_type: PyDataType,
        aggregation_bool: bool,
    ) -> Self {
        let mut func = Self {
            name: function_name,
            return_types: HashMap::new(),
            aggregation: aggregation_bool,
        };
        func.add_type_mapping(input_types, return_type);
        func
    }

    pub fn add_type_mapping(&mut self, input_types: Vec<PyDataType>, return_type: PyDataType) {
        self.return_types.insert(
            input_types.iter().map(|t| t.clone().into()).collect(),
            return_type.into(),
        );
    }
}
