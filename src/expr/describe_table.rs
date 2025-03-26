use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use arrow::{datatypes::Schema, pyarrow::PyArrowType};
use datafusion::logical_expr::DescribeTable;
use pyo3::prelude::*;

use crate::{common::df_schema::PyDFSchema, sql::logical::PyLogicalPlan};

use super::logical_node::LogicalNode;

#[pyclass(name = "DescribeTable", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyDescribeTable {
    describe: DescribeTable,
}

impl Display for PyDescribeTable {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DescribeTable")
    }
}

#[pymethods]
impl PyDescribeTable {
    #[new]
    fn new(schema: PyArrowType<Schema>, output_schema: PyDFSchema) -> Self {
        Self {
            describe: DescribeTable {
                schema: Arc::new(schema.0),
                output_schema: Arc::new(output_schema.into()),
            },
        }
    }

    pub fn schema(&self) -> PyArrowType<Schema> {
        (*self.describe.schema).clone().into()
    }

    pub fn output_schema(&self) -> PyDFSchema {
        (*self.describe.output_schema).clone().into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("DescribeTable({})", self))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("DescribeTable".to_string())
    }
}

impl From<PyDescribeTable> for DescribeTable {
    fn from(describe: PyDescribeTable) -> Self {
        describe.describe
    }
}

impl From<DescribeTable> for PyDescribeTable {
    fn from(describe: DescribeTable) -> PyDescribeTable {
        PyDescribeTable { describe }
    }
}

impl LogicalNode for PyDescribeTable {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        Ok(self.clone().into_py(py))
    }
}
