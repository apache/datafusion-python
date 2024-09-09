use std::{
    ffi::{c_void, CString},
    sync::Arc,
};

use datafusion::{physical_expr::{EquivalenceProperties, LexOrdering}, physical_plan::{DisplayAs, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties}};

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_ExecutionPlan {
    pub properties: Option<unsafe extern "C" fn(provider: *const FFI_ExecutionPlan) -> FFI_ArrowSchema>,

    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_ExecutionPlan {}
unsafe impl Sync for FFI_ExecutionPlan {}

pub struct ExecutionPlanPrivateData {
    pub plan: Arc<dyn ExecutionPlan + Send>,
    pub last_error: Option<CString>,
}

struct ExportedExecutionPlan(*const FFI_ExecutionPlan);

impl FFI_ExecutionPlan {

    pub fn empty() -> Self {
        Self {
            private_data: std::ptr::null_mut(),
        }
    }
}

impl FFI_ExecutionPlan {
    pub fn new(plan: Arc<dyn ExecutionPlan + Send>) -> Self {
        let private_data = Box::new(ExecutionPlanPrivateData {
            plan,
            last_error: None,
        });

        Self {
            private_data: Box::into_raw(private_data) as *mut c_void
        }
    }
}

impl ExecutionPlan for FFI_ExecutionPlan {
    fn name(&self) -> &str {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        todo!()
    }
}

impl DisplayAs for FFI_ExecutionPlan {
    fn fmt_as(&self, t: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        todo!()
    }
}






#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_PlanProperties {
    /// See [ExecutionPlanProperties::equivalence_properties]
    pub eq_properties: EquivalenceProperties,
    /// See [ExecutionPlanProperties::output_partitioning]
    pub partitioning: Partitioning,
    /// See [ExecutionPlanProperties::execution_mode]
    pub execution_mode: ExecutionMode,
    /// See [ExecutionPlanProperties::output_ordering]
    output_ordering: Option<LexOrdering>,
}