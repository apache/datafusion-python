use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use datafusion::logical_expr::DropFunction;
use pyo3::prelude::*;

use super::logical_node::LogicalNode;
use crate::common::df_schema::PyDFSchema;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "DropFunction", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyDropFunction {
    drop: DropFunction,
}

impl From<PyDropFunction> for DropFunction {
    fn from(drop: PyDropFunction) -> Self {
        drop.drop
    }
}

impl From<DropFunction> for PyDropFunction {
    fn from(drop: DropFunction) -> PyDropFunction {
        PyDropFunction { drop }
    }
}

impl Display for PyDropFunction {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DropFunction")
    }
}

#[pymethods]
impl PyDropFunction {
    #[new]
    fn new(name: String, schema: PyDFSchema, if_exists: bool) -> PyResult<Self> {
        Ok(PyDropFunction {
            drop: DropFunction {
                name,
                schema: Arc::new(schema.into()),
                if_exists,
            },
        })
    }
    fn name(&self) -> PyResult<String> {
        Ok(self.drop.name.clone())
    }

    fn schema(&self) -> PyDFSchema {
        (*self.drop.schema).clone().into()
    }

    fn if_exists(&self) -> PyResult<bool> {
        Ok(self.drop.if_exists)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("DropFunction({})", self))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("DropFunction".to_string())
    }
}

impl LogicalNode for PyDropFunction {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.clone().into_py(py))
    }
}
