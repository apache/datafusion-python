use std::sync::Arc;

use datafusion::logical_expr::Values;
use pyo3::{pyclass, Bound, IntoPyObjectExt, PyAny, PyErr, PyResult, Python};
use pyo3::prelude::*;

use crate::{common::df_schema::PyDFSchema, sql::logical::PyLogicalPlan};

use super::{logical_node::LogicalNode, PyExpr};

#[pyclass(name = "Values", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyValues {
    values: Values
}

impl From<Values> for PyValues {
    fn from(values: Values) -> PyValues {
        PyValues { values }
    }
}

impl TryFrom<PyValues> for Values {
    type Error = PyErr;

    fn try_from(py: PyValues) -> Result<Self, Self::Error> {
        Ok(py.values)
    }
}

impl LogicalNode for PyValues {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        Ok(self.clone().into_py(py))
    }
}

#[pymethods]
impl PyValues {

    #[new]
    pub fn new(schema: PyDFSchema, values: Vec<Vec<PyExpr>>) -> PyResult<Self> {
        let values = values.into_iter().map(|row| row.into_iter().map(|expr| expr.into()).collect()).collect();
        Ok(PyValues { values: Values {schema: Arc::new(schema.into()), values} })
    }

    pub fn schema(&self) -> PyResult<PyDFSchema> {
        Ok((*self.values.schema).clone().into())
    }

    pub fn values(&self) -> Vec<Vec<PyExpr>> {
        self.values.values.clone().into_iter().map(|row| row.into_iter().map(|expr| expr.into()).collect()).collect()
    }
}

