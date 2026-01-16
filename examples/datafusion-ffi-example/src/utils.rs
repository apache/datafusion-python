use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::{PyAnyMethods, PyCapsuleMethods};
use pyo3::types::PyCapsule;
use pyo3::{Bound, PyAny, PyResult};

pub(crate) fn ffi_logical_codec_from_pycapsule(
    obj: &Bound<PyAny>,
) -> PyResult<FFI_LogicalExtensionCodec> {
    let attr_name = "__datafusion_logical_extension_codec__";

    if obj.hasattr(attr_name)? {
        let capsule = obj.getattr(attr_name)?.call0()?;
        let capsule = capsule.downcast::<PyCapsule>()?;
        validate_pycapsule(capsule, "datafusion_logical_extension_codec")?;

        let provider = unsafe { capsule.reference::<FFI_LogicalExtensionCodec>() };

        Ok(provider.clone())
    } else {
        Err(PyValueError::new_err(
            "Expected PyCapsule object for FFI_LogicalExtensionCodec, but attribute does not exist",
        ))
    }
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
