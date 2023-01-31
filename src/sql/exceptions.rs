use std::fmt::Debug;

use pyo3::{create_exception, PyErr};

// Identifies exceptions that occur while attempting to generate a `LogicalPlan` from a SQL string
create_exception!(rust, ParsingException, pyo3::exceptions::PyException);

// Identifies exceptions that occur during attempts to optimization an existing `LogicalPlan`
create_exception!(rust, OptimizationException, pyo3::exceptions::PyException);

pub fn py_type_err(e: impl Debug) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!("{:?}", e))
}

pub fn py_runtime_err(e: impl Debug) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e))
}

pub fn py_parsing_exp(e: impl Debug) -> PyErr {
    PyErr::new::<ParsingException, _>(format!("{:?}", e))
}

pub fn py_optimization_exp(e: impl Debug) -> PyErr {
    PyErr::new::<OptimizationException, _>(format!("{:?}", e))
}
