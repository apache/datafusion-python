use std::{
    ffi::{c_void, CString},
    sync::Arc,
};

use datafusion::physical_plan::ExecutionPlan;

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_ExecutionPlan {
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_ExecutionPlan {}

pub struct ExecutionPlanPrivateData {
    pub plan: Arc<dyn ExecutionPlan + Send>,
    pub last_error: Option<CString>,
}
