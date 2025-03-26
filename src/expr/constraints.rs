use std::fmt::{self, Display, Formatter};

use datafusion::common::Constraints;
use pyo3::prelude::*;

#[pyclass(name = "Constraints", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyConstraints {
    pub constraints: Constraints,
}

impl From<PyConstraints> for Constraints {
    fn from(constraints: PyConstraints) -> Self {
        constraints.constraints
    }
}

impl From<Constraints> for PyConstraints {
    fn from(constraints: Constraints) -> Self {
        PyConstraints { constraints }
    }
}

impl Display for PyConstraints {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Constraints: {:?}", self.constraints)
    }
}

