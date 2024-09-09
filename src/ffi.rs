use std::{
    ffi::{c_char, c_int, c_void, CStr, CString},
    ptr::addr_of,
    sync::Arc,
};

use arrow::{error::ArrowError, ffi::FFI_ArrowSchema};
use datafusion::common::Result;
use datafusion::{
    catalog::{Session, TableProvider},
    common::DFSchema,
    execution::{context::SessionState, session_state::SessionStateBuilder},
    physical_plan::ExecutionPlan,
    prelude::{Expr, SessionConfig},
};
use tokio::runtime::Runtime;

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub enum FFI_Constraint {
    /// Columns with the given indices form a composite primary key (they are
    /// jointly unique and not nullable):
    PrimaryKey(Vec<usize>),
    /// Columns with the given indices form a composite unique key:
    Unique(Vec<usize>),
}

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_ExecutionPlan {
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_ExecutionPlan {}

struct ExecutionPlanPrivateData {
    plan: Arc<dyn ExecutionPlan + Send>,
    last_error: Option<CString>,
}

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_SessionConfig {
    pub version: i64,

    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_SessionConfig {}

struct SessionConfigPrivateData {
    config: SessionConfig,
    last_error: Option<CString>,
}

struct ExportedSessionConfig {
    session: *mut FFI_SessionConfig,
}

impl ExportedSessionConfig {
    fn get_private_data(&mut self) -> &mut SessionConfigPrivateData {
        unsafe { &mut *((*self.session).private_data as *mut SessionConfigPrivateData) }
    }
}

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_Expr {}

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_TableProvider {
    pub version: i64,
    pub schema: Option<
        unsafe extern "C" fn(provider: *mut FFI_TableProvider, out: *mut FFI_ArrowSchema) -> c_int,
    >,
    pub scan: Option<
        unsafe extern "C" fn(
            provider: *mut FFI_TableProvider,
            session_config: *mut FFI_SessionConfig,
            n_projections: c_int,
            projections: *mut c_int,
            n_filters: c_int,
            filters: *mut *const c_char,
            limit: c_int,
            out: *mut FFI_ExecutionPlan,
        ) -> c_int,
    >,
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_TableProvider {}

struct ProviderPrivateData {
    provider: Box<dyn TableProvider + Send>,
    last_error: Option<CString>,
}

struct ExportedTableProvider {
    provider: *mut FFI_TableProvider,
}

// The callback used to get array schema
unsafe extern "C" fn provider_schema(
    provider: *mut FFI_TableProvider,
    schema: *mut FFI_ArrowSchema,
) -> c_int {
    ExportedTableProvider { provider }.schema(schema)
}

unsafe extern "C" fn provider_scan(
    provider: *mut FFI_TableProvider,
    session_config: *mut FFI_SessionConfig,
    n_projections: c_int,
    projections: *mut c_int,
    n_filters: c_int,
    filters: *mut *const c_char,
    limit: c_int,
    mut out: *mut FFI_ExecutionPlan,
) -> c_int {
    let config = unsafe { (*session_config).private_data as *const SessionConfigPrivateData };
    let session = SessionStateBuilder::new()
        .with_config((*config).config.clone())
        .build();

    let num_projections: usize = n_projections.try_into().unwrap_or(0);

    let projections: Vec<usize> = std::slice::from_raw_parts(projections, num_projections)
        .iter()
        .filter_map(|v| (*v).try_into().ok())
        .collect();
    let maybe_projections = match projections.is_empty() {
        true => None,
        false => Some(&projections),
    };

    let filters_slice = std::slice::from_raw_parts(filters, n_filters as usize);
    let filters_vec: Vec<String> = filters_slice
        .iter()
        .map(|&s| CStr::from_ptr(s).to_string_lossy().to_string())
        .collect();

    let limit = limit.try_into().ok();

    let plan =
        ExportedTableProvider { provider }.scan(&session, maybe_projections, filters_vec, limit);

    match plan {
        Ok(mut plan) => {
            out = &mut plan;
            0
        }
        Err(_) => 1,
    }
}

impl ExportedTableProvider {
    fn get_private_data(&mut self) -> &mut ProviderPrivateData {
        unsafe { &mut *((*self.provider).private_data as *mut ProviderPrivateData) }
    }

    pub fn schema(&mut self, out: *mut FFI_ArrowSchema) -> i32 {
        let private_data = self.get_private_data();
        let provider = &private_data.provider;

        let schema = FFI_ArrowSchema::try_from(provider.schema().as_ref());

        match schema {
            Ok(schema) => {
                unsafe { std::ptr::copy(addr_of!(schema), out, 1) };
                std::mem::forget(schema);
                0
            }
            Err(ref err) => {
                private_data.last_error = Some(
                    CString::new(err.to_string()).expect("Error string has a null byte in it."),
                );
                get_error_code(err)
            }
        }
    }

    pub fn scan(
        &mut self,
        session: &SessionState,
        projections: Option<&Vec<usize>>,
        filters: Vec<String>,
        limit: Option<usize>,
    ) -> Result<FFI_ExecutionPlan> {
        let private_data = self.get_private_data();
        let provider = &private_data.provider;

        let schema = provider.schema();
        let df_schema: DFSchema = schema.try_into()?;

        let filter_exprs = filters
            .into_iter()
            .map(|expr_str| session.create_logical_expr(&expr_str, &df_schema))
            .collect::<datafusion::common::Result<Vec<Expr>>>()?;

        let runtime = Runtime::new().unwrap();
        let plan = runtime.block_on(provider.scan(session, projections, &filter_exprs, limit))?;

        let plan_ptr = Box::new(ExecutionPlanPrivateData {
            plan,
            last_error: None,
        });

        Ok(FFI_ExecutionPlan {
            private_data: Box::into_raw(plan_ptr) as *mut c_void,
        })
    }
}

const ENOMEM: i32 = 12;
const EIO: i32 = 5;
const EINVAL: i32 = 22;
const ENOSYS: i32 = 78;

fn get_error_code(err: &ArrowError) -> i32 {
    match err {
        ArrowError::NotYetImplemented(_) => ENOSYS,
        ArrowError::MemoryError(_) => ENOMEM,
        ArrowError::IoError(_, _) => EIO,
        _ => EINVAL,
    }
}

impl FFI_TableProvider {
    /// Creates a new [`FFI_TableProvider`].
    pub fn new(provider: Box<dyn TableProvider + Send>) -> Self {
        let private_data = Box::new(ProviderPrivateData {
            provider,
            last_error: None,
        });

        Self {
            version: 2,
            schema: Some(provider_schema),
            scan: Some(provider_scan),
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }

    /**
        Replace temporary pointer with updated
        # Safety
        User must validate the raw pointer is valid.
    */
    pub unsafe fn from_raw(raw_provider: *mut FFI_TableProvider) -> Self {
        std::ptr::replace(raw_provider, Self::empty())
    }

    /// Creates a new empty [FFI_ArrowArrayStream]. Used to import from the C Stream Interface.
    pub fn empty() -> Self {
        Self {
            version: 0,
            schema: None,
            scan: None,
            private_data: std::ptr::null_mut(),
        }
    }
}
