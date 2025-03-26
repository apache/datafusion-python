use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{DmlStatement, WriteOp};
use pyo3::prelude::*;

use crate::{common::df_schema::PyDFSchema, sql::logical::PyLogicalPlan};

use super::logical_node::LogicalNode;

#[pyclass(name = "DmlStatement", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyDmlStatement {
    dml: DmlStatement,
}

impl From<PyDmlStatement> for DmlStatement {
    fn from(dml: PyDmlStatement) -> Self {
        dml.dml
    }
}

impl From<DmlStatement> for PyDmlStatement {
    fn from(dml: DmlStatement) -> PyDmlStatement {
        PyDmlStatement { dml }
    }
}

impl LogicalNode for PyDmlStatement {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.dml.input).clone())]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        Ok(self.clone().into_py(py))
    }
}

#[pymethods]
impl PyDmlStatement {
    pub fn table_name(&self) -> PyResult<String> {
        Ok(self.dml.table_name.to_string())
    }

    pub fn table_schema(&self) -> PyDFSchema {
        (*self.dml.table_schema).clone().into()
    }

    pub fn op(&self) -> PyWriteOp {
        self.dml.op.clone().into()
    }

    pub fn input(&self) -> PyLogicalPlan {
        PyLogicalPlan {
            plan: self.dml.input.clone(),
        }
    }

    pub fn output_schema(&self) -> PyDFSchema {
        (*self.dml.output_schema).clone().into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok("DmlStatement".to_string())
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("DmlStatement".to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(eq, eq_int, name = "WriteOp", module = "datafusion.expr")]
pub enum PyWriteOp {
    Append,
    Overwrite,
    Replace,

    Update,
    Delete,
    Ctas,
}

impl From<WriteOp> for PyWriteOp {
    fn from(write_op: WriteOp) -> Self {
        match write_op {
            WriteOp::Insert(InsertOp::Append) => PyWriteOp::Append,
            WriteOp::Insert(InsertOp::Overwrite) => PyWriteOp::Overwrite,
            WriteOp::Insert(InsertOp::Replace) => PyWriteOp::Replace,

            WriteOp::Update => PyWriteOp::Update,
            WriteOp::Delete => PyWriteOp::Delete,
            WriteOp::Ctas => PyWriteOp::Ctas,
        }
    }
}

impl From<PyWriteOp> for WriteOp {
    fn from(py: PyWriteOp) -> Self {
        match py {
            PyWriteOp::Append => WriteOp::Insert(InsertOp::Append),
            PyWriteOp::Overwrite => WriteOp::Insert(InsertOp::Overwrite),
            PyWriteOp::Replace => WriteOp::Insert(InsertOp::Replace),

            PyWriteOp::Update => WriteOp::Update,
            PyWriteOp::Delete => WriteOp::Delete,
            PyWriteOp::Ctas => WriteOp::Ctas,
        }
    }
}

#[pymethods]
impl PyWriteOp {
    fn name(&self) -> String {
        let write_op: WriteOp = self.clone().into();
        write_op.name().to_string()
    }
}
