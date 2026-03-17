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

use pyo3::prelude::*;

pub mod data_type;
pub mod df_schema;
pub mod function;
pub mod schema;

/// Initializes the `common` module to match the pattern of `datafusion-common` https://docs.rs/datafusion-common/18.0.0/datafusion_common/index.html
pub(crate) fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<df_schema::PyDFSchema>()?;
    m.add_class::<data_type::PyDataType>()?;
    m.add_class::<data_type::DataTypeMap>()?;
    m.add_class::<data_type::RexType>()?;
    m.add_class::<data_type::PythonType>()?;
    m.add_class::<data_type::SqlType>()?;
    m.add_class::<data_type::NullTreatment>()?;
    m.add_class::<schema::SqlTable>()?;
    m.add_class::<schema::SqlSchema>()?;
    m.add_class::<schema::SqlView>()?;
    m.add_class::<schema::SqlStatistics>()?;
    m.add_class::<function::SqlFunction>()?;
    m.add_class::<schema::PyTableType>()?;
    m.add_class::<schema::PyTableSource>()?;
    m.add_class::<schema::PyConstraints>()?;
    Ok(())
}
