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

//! Example storage library: a partitioned Parquet table provider, its own
//! execution plan node, and a physical codec that writes durable metadata.
//!
//! One of three libraries in `examples/distributed`. This one owns tables.

use pyo3::prelude::*;

use crate::extension::{BundledPhysicalCodec, DfxStorageExtension};
use crate::table_provider::PyPartitionedParquetTable;

mod codec;
mod exec;
mod extension;
mod table_provider;

#[pymodule]
fn dfx_storage(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<BundledPhysicalCodec>()?;
    m.add_class::<DfxStorageExtension>()?;
    m.add_class::<PyPartitionedParquetTable>()?;
    Ok(())
}
