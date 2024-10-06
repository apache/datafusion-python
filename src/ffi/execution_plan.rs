use std::{
    ffi::{c_void, CString},
    ptr::null,
    sync::Arc,
};

use datafusion::error::Result;
use datafusion::{
    error::DataFusionError,
    physical_plan::{DisplayAs, ExecutionMode, ExecutionPlan, PlanProperties},
};

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFIExecutionPlan {
    pub properties:
        Option<unsafe extern "C" fn(plan: *const FFIExecutionPlan) -> FFIPlanProperties>,
    pub children: Option<
        unsafe extern "C" fn(
            plan: *const FFIExecutionPlan,
            num_children: &mut usize,
            out: &mut *const FFIExecutionPlan,
        ) -> i32,
    >,

    pub private_data: *mut c_void,
}

pub struct ExecutionPlanPrivateData {
    pub plan: Arc<dyn ExecutionPlan + Send>,
    pub last_error: Option<CString>,
}

unsafe extern "C" fn properties_fn_wrapper(plan: *const FFIExecutionPlan) -> FFIPlanProperties {
    let private_data = (*plan).private_data as *const ExecutionPlanPrivateData;
    let properties = (*private_data).plan.properties();
    properties.into()
}

unsafe extern "C" fn children_fn_wrapper(
    plan: *const FFIExecutionPlan,
    num_children: &mut usize,
    out: &mut *const FFIExecutionPlan,
) -> i32 {
    let private_data = (*plan).private_data as *const ExecutionPlanPrivateData;

    let children = (*private_data).plan.children();
    *num_children = children.len();
    let children: Vec<FFIExecutionPlan> = children
        .into_iter()
        .map(|child| FFIExecutionPlan::new(child.clone()))
        .collect();
    *out = children.as_ptr();

    0
}

// Since the trait ExecutionPlan requires borrowed values, we wrap our FFI.
// This struct exists on the consumer side (datafusion-python, for example) and not
// in the provider's side.
#[derive(Debug)]
pub struct ExportedExecutionPlan {
    plan: *const FFIExecutionPlan,
    properties: PlanProperties,
    children: Vec<Arc<dyn ExecutionPlan>>,
}

unsafe impl Send for ExportedExecutionPlan {}
unsafe impl Sync for ExportedExecutionPlan {}

impl DisplayAs for ExportedExecutionPlan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "FFIExecutionPlan(number_of_children={})",
            self.children.len(),
        )
    }
}

impl FFIExecutionPlan {
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
    /// Wrap a FFI Execution Plan
    ///
    /// # Safety
    ///
    /// The caller must ensure the pointer provided points to a valid implementation
    /// of FFIExecutionPlan
    pub unsafe fn new(plan: *const FFIExecutionPlan) -> Result<Self> {
        let properties = unsafe {
            let properties_fn = (*plan).properties.ok_or(DataFusionError::NotImplemented(
                "properties not implemented on FFIExecutionPlan".to_string(),
            ))?;
            properties_fn(plan).into()
        };

        let children = unsafe {
            let children_fn = (*plan).children.ok_or(DataFusionError::NotImplemented(
                "children not implemented on FFIExecutionPlan".to_string(),
            ))?;
            let mut num_children = 0;
            let mut children_ptr: *const FFIExecutionPlan = null();

            if children_fn(plan, &mut num_children, &mut children_ptr) != 0 {
                return Err(DataFusionError::Plan(
                    "Error getting children for FFIExecutionPlan".to_string(),
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

impl DisplayAs for FFIExecutionPlan {
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
pub struct FFIPlanProperties {
    // We will build equivalence properties from teh schema and ordersing (new_with_orderings). This is how we do ti in dataset_exec
    // pub eq_properties: Option<unsafe extern "C" fn(plan: *const FFIPlanProperties) -> EquivalenceProperties>,

    // Returns protobuf serialized bytes of the partitioning
    pub output_partitioning: Option<
        unsafe extern "C" fn(
            plan: *const FFIPlanProperties,
            buffer_size: &mut usize,
            buffer_bytes: &mut *mut u8,
        ) -> i32,
    >,

    pub execution_mode:
        Option<unsafe extern "C" fn(plan: *const FFIPlanProperties) -> FFIExecutionMode>,

    // PhysicalSortExprNodeCollection proto
    pub output_ordering: Option<
        unsafe extern "C" fn(
            plan: *const FFIPlanProperties,
            buffer_size: &mut usize,
            buffer_bytes: &mut *mut u8,
        ) -> i32,
    >,
}

impl From<&PlanProperties> for FFIPlanProperties {
    fn from(value: &PlanProperties) -> Self {
        todo!()
    }
}

impl From<FFIPlanProperties> for PlanProperties {
    fn from(value: FFIPlanProperties) -> Self {
        todo!()
    }
}

#[repr(C)]
pub enum FFIExecutionMode {
    Bounded,

    Unbounded,

    PipelineBreaking,
}

impl From<ExecutionMode> for FFIExecutionMode {
    fn from(value: ExecutionMode) -> Self {
        match value {
            ExecutionMode::Bounded => FFIExecutionMode::Bounded,
            ExecutionMode::Unbounded => FFIExecutionMode::Unbounded,
            ExecutionMode::PipelineBreaking => FFIExecutionMode::PipelineBreaking,
        }
    }
}

impl From<FFIExecutionMode> for ExecutionMode {
    fn from(value: FFIExecutionMode) -> Self {
        match value {
            FFIExecutionMode::Bounded => ExecutionMode::Bounded,
            FFIExecutionMode::Unbounded => ExecutionMode::Unbounded,
            FFIExecutionMode::PipelineBreaking => ExecutionMode::PipelineBreaking,
        }
    }
}
