use std::{
    ffi::{c_void, CString},
    ptr::null,
    sync::Arc,
};

use datafusion::error::Result;
use datafusion::{
    error::DataFusionError,
    parquet::file::properties,
    physical_expr::{EquivalenceProperties, LexOrdering},
    physical_plan::{DisplayAs, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties},
};

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_ExecutionPlan {
    pub properties:
        Option<unsafe extern "C" fn(plan: *const FFI_ExecutionPlan) -> FFI_PlanProperties>,
    pub children: Option<
        unsafe extern "C" fn(
            plan: *const FFI_ExecutionPlan,
            num_children: &mut usize,
            out: &mut *const FFI_ExecutionPlan,
        ) -> i32,
    >,

    pub private_data: *mut c_void,
}

pub struct ExecutionPlanPrivateData {
    pub plan: Arc<dyn ExecutionPlan + Send>,
    pub last_error: Option<CString>,
}

unsafe extern "C" fn properties_fn_wrapper(plan: *const FFI_ExecutionPlan) -> FFI_PlanProperties {
    let private_data = (*plan).private_data as *const ExecutionPlanPrivateData;
    let properties = (*private_data).plan.properties();
    properties.into()
}

unsafe extern "C" fn children_fn_wrapper(
    plan: *const FFI_ExecutionPlan,
    num_children: &mut usize,
    out: &mut *const FFI_ExecutionPlan,
) -> i32 {
    let private_data = (*plan).private_data as *const ExecutionPlanPrivateData;

    let children = (*private_data).plan.children();
    *num_children = children.len();
    let children: Vec<FFI_ExecutionPlan> = children
        .into_iter()
        .map(|child| FFI_ExecutionPlan::new(child.clone()))
        .collect();
    *out = children.as_ptr();

    0
}

// Since the trait ExecutionPlan requires borrowed values, we wrap our FFI.
// This struct exists on the consumer side (datafusion-python, for example) and not
// in the provider's side.
#[derive(Debug)]
pub struct ExportedExecutionPlan {
    plan: *const FFI_ExecutionPlan,
    properties: PlanProperties,
    children: Vec<Arc<dyn ExecutionPlan>>,
}

unsafe impl Send for ExportedExecutionPlan {}
unsafe impl Sync for ExportedExecutionPlan {}

impl DisplayAs for ExportedExecutionPlan {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        todo!()
    }
}

impl FFI_ExecutionPlan {
    pub fn new(plan: Arc<dyn ExecutionPlan + Send>) -> Self {
        let private_data = Box::new(ExecutionPlanPrivateData {
            plan,
            last_error: None,
        });

        Self {
            properties: Some(properties_fn_wrapper),
            children: Some(children_fn_wrapper),
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }

    pub fn empty() -> Self {
        Self {
            properties: None,
            children: None,
            private_data: std::ptr::null_mut(),
        }
    }
}

impl ExportedExecutionPlan {
    pub fn new(plan: *const FFI_ExecutionPlan) -> Result<Self> {
        let properties = unsafe {
            let properties_fn = (*plan).properties.ok_or(DataFusionError::NotImplemented(
                "properties not implemented on FFI_ExecutionPlan".to_string(),
            ))?;
            properties_fn(plan).into()
        };

        let children = unsafe {
            let children_fn = (*plan).children.ok_or(DataFusionError::NotImplemented(
                "children not implemented on FFI_ExecutionPlan".to_string(),
            ))?;
            let mut num_children = 0;
            let mut children_ptr: *const FFI_ExecutionPlan = null();

            if children_fn(plan, &mut num_children, &mut children_ptr) != 0 {
                return Err(DataFusionError::Plan(
                    "Error getting children for FFI_ExecutionPlan".to_string(),
                ));
            }

            let ffi_vec = Vec::from_raw_parts(&mut children_ptr, num_children, num_children);
            let maybe_children: Result<Vec<_>> = ffi_vec
                .into_iter()
                .map(|child| {
                    ExportedExecutionPlan::new(child).map(|c| Arc::new(c) as Arc<dyn ExecutionPlan>)
                })
                .collect();

            maybe_children?
        };

        Ok(Self {
            plan,
            properties,
            children,
        })
    }
}

impl ExecutionPlan for ExportedExecutionPlan {
    fn name(&self) -> &str {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
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
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
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

impl From<&PlanProperties> for FFI_PlanProperties {
    fn from(value: &PlanProperties) -> Self {
        todo!()
    }
}

impl From<FFI_PlanProperties> for PlanProperties {
    fn from(value: FFI_PlanProperties) -> Self {
        todo!()
    }
}
