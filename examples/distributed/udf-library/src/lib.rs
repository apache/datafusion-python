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

//! Example function library: a scalar UDF, an aggregate, a window function,
//! and the two codecs that let plans referencing them be read elsewhere.
//!
//! One of three libraries in `examples/distributed`. This one owns functions,
//! and is the one that cannot be installed with `with_extensions` -- see
//! [`crate::python`].

use pyo3::prelude::*;

use crate::python::{
    PyCodecObservations, PyLogicalCodec, PyNetRevenue, PyPhysicalCodec, PyRevenueRank,
    PyWeightedAvg,
};

mod codec;
mod functions;
mod python;

#[pymodule]
fn dfx_udfs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<PyCodecObservations>()?;
    m.add_class::<PyLogicalCodec>()?;
    m.add_class::<PyNetRevenue>()?;
    m.add_class::<PyPhysicalCodec>()?;
    m.add_class::<PyRevenueRank>()?;
    m.add_class::<PyWeightedAvg>()?;
    Ok(())
}
