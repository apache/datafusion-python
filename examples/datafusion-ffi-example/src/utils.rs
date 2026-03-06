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

use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use pyo3::exceptions::PyValueError;
use pyo3::ffi::c_str;
use pyo3::prelude::{PyAnyMethods, PyCapsuleMethods};
use pyo3::types::PyCapsule;
use pyo3::{Bound, PyAny, PyResult};
use std::ptr::NonNull;

pub(crate) fn ffi_logical_codec_from_pycapsule(
    obj: Bound<PyAny>,
) -> PyResult<FFI_LogicalExtensionCodec> {
    let attr_name = "__datafusion_logical_extension_codec__";
    let capsule = if obj.hasattr(attr_name)? {
        obj.getattr(attr_name)?.call0()?
    } else {
        obj
    };

    let capsule = capsule.cast::<PyCapsule>()?;
    validate_pycapsule(capsule, "datafusion_logical_extension_codec")?;

    let data: NonNull<FFI_LogicalExtensionCodec> = capsule
        .pointer_checked(Some(c_str!("datafusion_logical_extension_codec")))?
        .cast();
    let codec = unsafe { data.as_ref() };

    Ok(codec.clone())
}

pub(crate) fn validate_pycapsule(capsule: &Bound<PyCapsule>, name: &str) -> PyResult<()> {
    let capsule_name = capsule.name()?;
    if capsule_name.is_none() {
        return Err(PyValueError::new_err(format!(
            "Expected {name} PyCapsule to have name set."
        )));
    }

    let capsule_name = unsafe { capsule_name.unwrap().as_cstr().to_str()? };
    if capsule_name != name {
        return Err(PyValueError::new_err(format!(
            "Expected name '{name}' in PyCapsule, instead got '{capsule_name}'"
        )));
    }

    Ok(())
}
