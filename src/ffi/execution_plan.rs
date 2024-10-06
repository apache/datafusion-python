use std::{
    ffi::{c_void, CString},
    ptr::{null, null_mut},
    slice,
    sync::Arc,
};

use arrow::{datatypes::Schema, ffi::FFI_ArrowSchema};
use datafusion::{
    error::DataFusionError,
    physical_plan::{DisplayAs, ExecutionMode, ExecutionPlan, PlanProperties},
};
use datafusion::{error::Result, physical_expr::EquivalenceProperties, prelude::SessionContext};
use datafusion_proto::{
    physical_plan::{
        from_proto::{parse_physical_sort_exprs, parse_protobuf_partitioning},
        DefaultPhysicalExtensionCodec,
    },
    protobuf::{partitioning, Partitioning, PhysicalSortExprNodeCollection},
};
use prost::{DecodeError, Message};

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
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "FFI_ExecutionPlan(number_of_children={})",
            self.children.len(),
        )
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
    /// Wrap a FFI Execution Plan
    ///
    /// # Safety
    ///
    /// The caller must ensure the pointer provided points to a valid implementation
    /// of FFI_ExecutionPlan
    pub unsafe fn new(plan: *const FFI_ExecutionPlan) -> Result<Self> {
        let properties = unsafe {
            let properties_fn = (*plan).properties.ok_or(DataFusionError::NotImplemented(
                "properties not implemented on FFI_ExecutionPlan".to_string(),
            ))?;
            properties_fn(plan).try_into()?
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
pub struct FFI_PlanProperties {
    // We will build equivalence properties from teh schema and ordersing (new_with_orderings). This is how we do ti in dataset_exec
    // pub eq_properties: Option<unsafe extern "C" fn(plan: *const FFI_PlanProperties) -> EquivalenceProperties>,

    // Returns protobuf serialized bytes of the partitioning
    pub output_partitioning: Option<
        unsafe extern "C" fn(
            plan: *const FFI_PlanProperties,
            buffer_size: &mut usize,
            buffer_bytes: &mut *mut u8,
        ) -> i32,
    >,

    pub execution_mode:
        Option<unsafe extern "C" fn(plan: *const FFI_PlanProperties) -> FFI_ExecutionMode>,

    // PhysicalSortExprNodeCollection proto
    pub output_ordering: Option<
        unsafe extern "C" fn(
            plan: *const FFI_PlanProperties,
            buffer_size: &mut usize,
            buffer_bytes: &mut *mut u8,
        ) -> i32,
    >,

    pub schema: Option<unsafe extern "C" fn(plan: *const FFI_PlanProperties) -> FFI_ArrowSchema>,
}

impl From<&PlanProperties> for FFI_PlanProperties {
    fn from(value: &PlanProperties) -> Self {
        todo!()
    }
}

impl TryFrom<FFI_PlanProperties> for PlanProperties {
    type Error = DataFusionError;

    fn try_from(value: FFI_PlanProperties) -> std::result::Result<Self, Self::Error> {
        unsafe {
            let schema_fn = value.schema.ok_or(DataFusionError::NotImplemented(
                "schema() not implemented on FFI_PlanProperties".to_string(),
            ))?;
            let ffi_schema = schema_fn(&value);
            let schema: Schema = (&ffi_schema).try_into()?;

            let ordering_fn = value
                .output_ordering
                .ok_or(DataFusionError::NotImplemented(
                    "output_ordering() not implemented on FFI_PlanProperties".to_string(),
                ))?;
            let mut buff_size = 0;
            let mut buff = null_mut();
            if ordering_fn(&value, &mut buff_size, &mut buff) != 0 {
                return Err(DataFusionError::Plan(
                    "Error occurred during FFI call to output_ordering in FFI_PlanProperties"
                        .to_string(),
                ));
            }
            let data = slice::from_raw_parts(buff, buff_size);

            let proto_output_ordering = PhysicalSortExprNodeCollection::decode(data)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // TODO we will need to get these, but unsure if it happesn on the provider or consumer right now.
            let default_ctx = SessionContext::new();
            let codex = DefaultPhysicalExtensionCodec {};
            let orderings = parse_physical_sort_exprs(
                &proto_output_ordering.physical_sort_expr_nodes,
                &default_ctx,
                &schema,
                &codex,
            )?;

            let partitioning_fn =
                value
                    .output_partitioning
                    .ok_or(DataFusionError::NotImplemented(
                        "output_partitioning() not implemented on FFI_PlanProperties".to_string(),
                    ))?;
            if partitioning_fn(&value, &mut buff_size, &mut buff) != 0 {
                return Err(DataFusionError::Plan(
                    "Error occurred during FFI call to output_partitioning in FFI_PlanProperties"
                        .to_string(),
                ));
            }
            let data = slice::from_raw_parts(buff, buff_size);

            let proto_partitioning =
                Partitioning::decode(data).map_err(|e| DataFusionError::External(Box::new(e)))?;
            // TODO: Validate this unwrap is safe.
            let partitioning = parse_protobuf_partitioning(
                Some(&proto_partitioning),
                &default_ctx,
                &schema,
                &codex,
            )?
            .unwrap();

            let execution_mode_fn = value.execution_mode.ok_or(DataFusionError::NotImplemented(
                "execution_mode() not implemented on FFI_PlanProperties".to_string(),
            ))?;
            let execution_mode = execution_mode_fn(&value).into();

            let eq_properties =
                EquivalenceProperties::new_with_orderings(Arc::new(schema), &[orderings]);

            Ok(Self::new(eq_properties, partitioning, execution_mode))
        }
    }
    // fn from(value: FFI_PlanProperties) -> Self {
    //     let schema = self.schema()

    //     let equiv_prop = EquivalenceProperties::new_with_orderings(schema, orderings);
    // }
}

#[repr(C)]
pub enum FFI_ExecutionMode {
    Bounded,

    Unbounded,

    PipelineBreaking,
}

impl From<ExecutionMode> for FFI_ExecutionMode {
    fn from(value: ExecutionMode) -> Self {
        match value {
            ExecutionMode::Bounded => FFI_ExecutionMode::Bounded,
            ExecutionMode::Unbounded => FFI_ExecutionMode::Unbounded,
            ExecutionMode::PipelineBreaking => FFI_ExecutionMode::PipelineBreaking,
        }
    }
}

impl From<FFI_ExecutionMode> for ExecutionMode {
    fn from(value: FFI_ExecutionMode) -> Self {
        match value {
            FFI_ExecutionMode::Bounded => ExecutionMode::Bounded,
            FFI_ExecutionMode::Unbounded => ExecutionMode::Unbounded,
            FFI_ExecutionMode::PipelineBreaking => ExecutionMode::PipelineBreaking,
        }
    }
}
