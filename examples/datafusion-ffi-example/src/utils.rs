use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::{PyAnyMethods, PyCapsuleMethods};
use pyo3::types::PyCapsule;
use pyo3::{Bound, PyAny, PyResult};
//
// pub(crate) fn ffi_task_ctx_provider_from_pycapsule(
//     obj: &Bound<PyAny>,
// ) -> PyResult<FFI_TaskContextProvider> {
//     if obj.hasattr("__datafusion_task_context_provider__")? {
//         let capsule = obj
//             .getattr("__datafusion_task_context_provider__")?
//             .call0()?;
//         let capsule = capsule.downcast::<PyCapsule>()?;
//         validate_pycapsule(capsule, "datafusion_task_context_provider")?;
//
//         let provider = unsafe { capsule.reference::<FFI_TaskContextProvider>() };
//
//         Ok(provider.clone())
//     } else {
//         Err(PyValueError::new_err(
//             "Expected PyCapsule object for FFI_Session, but attribute does not exist",
//         ))
//     }
// }

macro_rules! ffi_from_pycapsule {
    ($fn_name:ident, $ffi_type:ty, $capsule_name:literal) => {
        pub(crate) fn $fn_name(obj: &Bound<PyAny>) -> PyResult<$ffi_type> {
            let attr_name = concat!("__", $capsule_name, "__");

            if obj.hasattr(attr_name)? {
                let capsule = obj.getattr(attr_name)?.call0()?;
                let capsule = capsule.downcast::<PyCapsule>()?;
                validate_pycapsule(capsule, $capsule_name)?;

                let provider = unsafe { capsule.reference::<$ffi_type>() };

                Ok(provider.clone())
            } else {
                Err(PyValueError::new_err(format!(
                    "Expected PyCapsule object for {}, but attribute does not exist",
                    stringify!($ffi_type)
                )))
            }
        }
    };
}

// Usage:
// ffi_from_pycapsule!(
//     ffi_task_ctx_provider_from_pycapsule,
//     FFI_TaskContextProvider,
//     "datafusion_task_context_provider"
// );

ffi_from_pycapsule!(
    ffi_logical_codec_from_pycapsule,
    FFI_LogicalExtensionCodec,
    "datafusion_logical_extension_codec"
);

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
