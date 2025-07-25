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

use crate::catalog_provider::MyCatalogProvider;
use crate::aggregate_udf::MySumUDF;
use crate::scalar_udf::IsNullUDF;
use crate::table_function::MyTableFunction;
use crate::table_provider::MyTableProvider;
use crate::window_udf::MyRankUDF;
use pyo3::prelude::*;

pub(crate) mod catalog_provider;
pub(crate) mod aggregate_udf;
pub(crate) mod scalar_udf;
pub(crate) mod table_function;
pub(crate) mod table_provider;
pub(crate) mod window_udf;

#[pymodule]
fn datafusion_ffi_example(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<MyTableProvider>()?;
    m.add_class::<MyTableFunction>()?;
    m.add_class::<MyCatalogProvider>()?;
    m.add_class::<IsNullUDF>()?;
    m.add_class::<MySumUDF>()?;
    m.add_class::<MyRankUDF>()?;
    Ok(())
}
