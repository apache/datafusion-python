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

use arrow::datatypes::Field;
use arrow::pyarrow::PyArrowType;
use datafusion::logical_expr::expr::Placeholder;
use pyo3::prelude::*;

use crate::common::data_type::PyDataType;

#[pyclass(frozen, name = "Placeholder", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyPlaceholder {
    placeholder: Placeholder,
}

impl From<Placeholder> for PyPlaceholder {
    fn from(placeholder: Placeholder) -> Self {
        PyPlaceholder { placeholder }
    }
}

#[pymethods]
impl PyPlaceholder {
    fn id(&self) -> String {
        self.placeholder.id.clone()
    }

    fn data_type(&self) -> Option<PyDataType> {
        self.placeholder
            .field
            .as_ref()
            .map(|f| f.data_type().clone().into())
    }

    fn field(&self) -> Option<PyArrowType<Field>> {
        self.placeholder
            .field
            .as_ref()
            .map(|f| f.as_ref().clone().into())
    }
}
