use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use datafusion::{common::SchemaReference, logical_expr::DropCatalogSchema, sql::TableReference};
use pyo3::{exceptions::PyValueError, prelude::*};

use crate::common::df_schema::PyDFSchema;

use super::logical_node::LogicalNode;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "DropCatalogSchema", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyDropCatalogSchema {
    drop: DropCatalogSchema,
}

impl From<PyDropCatalogSchema> for DropCatalogSchema {
    fn from(drop: PyDropCatalogSchema) -> Self {
        drop.drop
    }
}

impl From<DropCatalogSchema> for PyDropCatalogSchema {
    fn from(drop: DropCatalogSchema) -> PyDropCatalogSchema {
        PyDropCatalogSchema { drop }
    }
}

impl Display for PyDropCatalogSchema {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DropCatalogSchema")
    }
}

fn parse_schema_reference(name: String) -> PyResult<SchemaReference> {
    match name.into() {
        TableReference::Bare { table } => Ok(SchemaReference::Bare { schema: table }),
        TableReference::Partial { schema, table } => Ok(SchemaReference::Full {
            schema: table,
            catalog: schema,
        }),
        TableReference::Full {
            catalog: _,
            schema: _,
            table: _,
        } => Err(PyErr::new::<PyValueError, String>(
            "Invalid schema specifier (has 3 parts)".to_string(),
        )),
    }
}

#[pymethods]
impl PyDropCatalogSchema {
    #[new]
    fn new(name: String, schema: PyDFSchema, if_exists: bool, cascade: bool) -> PyResult<Self> {
        let name = parse_schema_reference(name)?;
        Ok(PyDropCatalogSchema {
            drop: DropCatalogSchema {
                name,
                schema: Arc::new(schema.into()),
                if_exists,
                cascade,
            },
        })
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

    fn cascade(&self) -> PyResult<bool> {
        Ok(self.drop.cascade)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("DropCatalogSchema({})", self))
    }
}

impl LogicalNode for PyDropCatalogSchema {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.clone().into_py(py))
    }
}
