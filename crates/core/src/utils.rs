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

use std::ptr::NonNull;
use std::sync::Arc;

use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
pub use datafusion_python_util::{
    create_logical_extension_capsule, get_global_ctx, get_tokio_runtime, is_ipython_env,
    parse_volatility, spawn_future, table_provider_from_pycapsule, validate_pycapsule,
    wait_for_future,
};
use pyo3::IntoPyObjectExt;
use pyo3::ffi::c_str;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::context::PySessionContext;
use crate::errors::py_datafusion_err;

pub(crate) fn extract_logical_extension_codec(
    py: Python,
    obj: Option<Bound<PyAny>>,
) -> PyResult<Arc<FFI_LogicalExtensionCodec>> {
    let obj = match obj {
        Some(obj) => obj,
        None => PySessionContext::global_ctx()?.into_bound_py_any(py)?,
    };
    let capsule = if obj.hasattr("__datafusion_logical_extension_codec__")? {
        obj.getattr("__datafusion_logical_extension_codec__")?
            .call0()?
    } else {
        obj
    };
    let capsule = capsule.cast::<PyCapsule>().map_err(py_datafusion_err)?;

    validate_pycapsule(capsule, "datafusion_logical_extension_codec")?;

    let data: NonNull<FFI_LogicalExtensionCodec> = capsule
        .pointer_checked(Some(c_str!("datafusion_logical_extension_codec")))?
        .cast();
    let codec = unsafe { data.as_ref() };
    Ok(Arc::new(codec.clone()))
}
