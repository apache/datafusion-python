use std::{fmt::{self, Display, Formatter}, sync::Arc};

use datafusion::logical_expr::DropView;
use pyo3::prelude::*;

use crate::common::df_schema::PyDFSchema;

use super::logical_node::LogicalNode;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "DropView", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyDropView {
    drop: DropView,
}


impl From<PyDropView> for DropView {
    fn from(drop: PyDropView) -> Self {
        drop.drop
    }
}

impl From<DropView> for PyDropView {
    fn from(drop: DropView) -> PyDropView {
        PyDropView { drop }
    }
}

impl Display for PyDropView {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DropView: {name:?} if not exist:={if_exists}", name = self.drop.name, if_exists = self.drop.if_exists)
    }
}

#[pymethods]
impl PyDropView {
    #[new]
    fn new(name: String, schema: PyDFSchema, if_exists: bool) -> PyResult<Self> {
        Ok(PyDropView { drop: DropView { name: name.into(), schema: Arc::new(schema.into()), if_exists } })
    }

    fn name(&self) -> PyResult<String> {
        Ok(self.drop.name.to_string())
    }

    fn schema(&self) -> PyDFSchema {
        (*self.drop.schema).clone().into()
    }

    fn if_exists(&self) -> PyResult<bool> {
        Ok(self.drop.if_exists)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("DropView({})", self))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("DropView".to_string())
    }
}

impl LogicalNode for PyDropView {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        Ok(self.clone().into_py(py))
    }
}
