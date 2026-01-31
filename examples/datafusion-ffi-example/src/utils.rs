use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::{PyAnyMethods, PyCapsuleMethods};
use pyo3::types::PyCapsule;
use pyo3::{Bound, PyAny, PyResult};

pub(crate) fn ffi_logical_codec_from_pycapsule(
    obj: Bound<PyAny>,
) -> PyResult<FFI_LogicalExtensionCodec> {
    let attr_name = "__datafusion_logical_extension_codec__";
    let capsule = if obj.hasattr(attr_name)? {
        obj.getattr(attr_name)?.call0()?
    } else {
        obj
    };

    let capsule = capsule.downcast::<PyCapsule>()?;
    validate_pycapsule(capsule, "datafusion_logical_extension_codec")?;

    let codec = unsafe { capsule.reference::<FFI_LogicalExtensionCodec>() };

    Ok(codec.clone())
}

pub(crate) fn validate_pycapsule(capsule: &Bound<PyCapsule>, name: &str) -> PyResult<()> {
    let capsule_name = capsule.name()?;
    if capsule_name.is_none() {
        return Err(PyValueError::new_err(format!(
            "Expected {name} PyCapsule to have name set."
        )));
    }

    let capsule_name = capsule_name.unwrap().to_str()?;
    if capsule_name != name {
        return Err(PyValueError::new_err(format!(
            "Expected name '{name}' in PyCapsule, instead got '{capsule_name}'"
        )));
    }

    Ok(())
}
