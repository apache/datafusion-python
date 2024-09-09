use std::ffi::{c_void, CString};

use datafusion::prelude::SessionConfig;

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_SessionConfig {
    pub version: i64,

    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_SessionConfig {}

pub struct SessionConfigPrivateData {
    pub config: SessionConfig,
    pub last_error: Option<CString>,
}

struct ExportedSessionConfig {
    session: *mut FFI_SessionConfig,
}

impl ExportedSessionConfig {
    fn get_private_data(&mut self) -> &mut SessionConfigPrivateData {
        unsafe { &mut *((*self.session).private_data as *mut SessionConfigPrivateData) }
    }
}
