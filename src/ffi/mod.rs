pub mod execution_plan;
pub mod session_config;
pub mod table_provider;

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
pub struct FFI_Expr {}
