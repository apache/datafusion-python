mod dialect;

use std::sync::Arc;

use datafusion::sql::unparser::{dialect::Dialect, Unparser};
use dialect::PyDialect;
use pyo3::{exceptions::PyValueError, prelude::*};

use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "Unparser", module = "datafusion.unparser", subclass)]
#[derive(Clone)]
pub struct PyUnparser {
    dialect: Arc<dyn Dialect>,
    pretty: bool,
}

#[pymethods]
impl PyUnparser {
    #[new]
    pub fn new(dialect: PyDialect) -> Self {
        Self {
            dialect: dialect.dialect.clone(),
            pretty: false,
        }
    }

    pub fn plan_to_sql(&self, plan: &PyLogicalPlan) -> PyResult<String> {
        let mut unparser = Unparser::new(self.dialect.as_ref());
        unparser = unparser.with_pretty(self.pretty);
        let sql = unparser
            .plan_to_sql(&plan.plan())
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(sql.to_string())
    }

    pub fn with_pretty(&self, pretty: bool) -> Self {
        Self {
            dialect: self.dialect.clone(),
            pretty,
        }
    }
}

pub(crate) fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyUnparser>()?;
    m.add_class::<PyDialect>()?;
    Ok(())
}
