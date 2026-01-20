// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::{PyArrowType, ToPyArrow};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    create_udaf, Accumulator, AccumulatorFactoryFunction, AggregateUDF,
};
use datafusion_ffi::udaf::{FFI_AggregateUDF, ForeignAggregateUDF};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict, PyTuple, PyType};

use crate::common::data_type::PyScalarValue;
use crate::errors::{py_datafusion_err, to_datafusion_err, PyDataFusionResult};
use crate::expr::PyExpr;
use crate::utils::{parse_volatility, validate_pycapsule};

#[derive(Debug)]
struct RustAccumulator {
    accum: Py<PyAny>,
    return_type: DataType,
    pyarrow_array_type: Option<Py<PyType>>,
    pyarrow_chunked_array_type: Option<Py<PyType>>,
}

impl RustAccumulator {
    fn new(accum: Py<PyAny>, return_type: DataType) -> Self {
        Self {
            accum,
            return_type,
            pyarrow_array_type: None,
            pyarrow_chunked_array_type: None,
        }
    }

    fn ensure_pyarrow_types(&mut self, py: Python<'_>) -> PyResult<(Py<PyType>, Py<PyType>)> {
        if self.pyarrow_array_type.is_none() || self.pyarrow_chunked_array_type.is_none() {
            let pyarrow = PyModule::import(py, "pyarrow")?;
            let array_attr = pyarrow.getattr("Array")?;
            let array_type = array_attr.downcast::<PyType>()?;
            let chunked_array_attr = pyarrow.getattr("ChunkedArray")?;
            let chunked_array_type = chunked_array_attr.downcast::<PyType>()?;
            self.pyarrow_array_type = Some(array_type.clone().unbind());
            self.pyarrow_chunked_array_type = Some(chunked_array_type.clone().unbind());
        }
        Ok((
            self.pyarrow_array_type
                .as_ref()
                .expect("array type set")
                .clone_ref(py),
            self.pyarrow_chunked_array_type
                .as_ref()
                .expect("chunked array type set")
                .clone_ref(py),
        ))
    }

    fn is_pyarrow_array_like(
        &mut self,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let (array_type, chunked_array_type) = self.ensure_pyarrow_types(py)?;
        let array_type = array_type.bind(py);
        let chunked_array_type = chunked_array_type.bind(py);
        Ok(value.is_instance(&array_type)? || value.is_instance(&chunked_array_type)?)
    }
}

impl Accumulator for RustAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Python::attach(|py| {
            self.accum
                .bind(py)
                .call_method0("state")?
                .extract::<Vec<PyScalarValue>>()
        })
        .map(|v| v.into_iter().map(|x| x.0).collect())
        .map_err(|e| DataFusionError::Execution(format!("{e}")))
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Python::attach(|py| {
            let value = self.accum.bind(py).call_method0("evaluate")?;
            let is_list_type = matches!(
                self.return_type,
                DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
            );
            if is_list_type && self.is_pyarrow_array_like(py, &value)? {
                let pyarrow = PyModule::import(py, "pyarrow")?;
                let list_value = value.call_method0("to_pylist")?;
                let py_type = self.return_type.to_pyarrow(py)?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("type", py_type)?;
                return pyarrow
                    .getattr("scalar")?
                    .call((list_value,), Some(&kwargs))?
                    .extract::<PyScalarValue>();
            }
            value.extract::<PyScalarValue>()
        })
        .map(|v| v.0)
        .map_err(|e| DataFusionError::Execution(format!("{e}")))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        Python::attach(|py| {
            // 1. cast args to Pyarrow array
            let py_args = values
                .iter()
                .map(|arg| arg.into_data().to_pyarrow(py).unwrap())
                .collect::<Vec<_>>();
            let py_args = PyTuple::new(py, py_args).map_err(to_datafusion_err)?;

            // 2. call function
            self.accum
                .bind(py)
                .call_method1("update", py_args)
                .map_err(|e| DataFusionError::Execution(format!("{e}")))?;

            Ok(())
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        Python::attach(|py| {
            // // 1. cast states to Pyarrow arrays
            let py_states: Result<Vec<Bound<'_, PyAny>>> = states
                .iter()
                .map(|state| {
                    state
                        .into_data()
                        .to_pyarrow(py)
                        .map_err(|e| DataFusionError::Execution(format!("{e}")))
                })
                .collect();

            // 2. call merge
            self.accum
                .bind(py)
                .call_method1("merge", (py_states?,))
                .map_err(|e| DataFusionError::Execution(format!("{e}")))?;

            Ok(())
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        Python::attach(|py| {
            // 1. cast args to Pyarrow array
            let py_args = values
                .iter()
                .map(|arg| arg.into_data().to_pyarrow(py).unwrap())
                .collect::<Vec<_>>();
            let py_args = PyTuple::new(py, py_args).map_err(to_datafusion_err)?;

            // 2. call function
            self.accum
                .bind(py)
                .call_method1("retract_batch", py_args)
                .map_err(|e| DataFusionError::Execution(format!("{e}")))?;

            Ok(())
        })
    }

    fn supports_retract_batch(&self) -> bool {
        Python::attach(
            |py| match self.accum.bind(py).call_method0("supports_retract_batch") {
                Ok(x) => x.extract().unwrap_or(false),
                Err(_) => false,
            },
        )
    }
}

pub fn to_rust_accumulator(accum: Py<PyAny>) -> AccumulatorFactoryFunction {
    Arc::new(move |args| -> Result<Box<dyn Accumulator>> {
        let accum = Python::attach(|py| {
            accum
                .call0(py)
                .map_err(|e| DataFusionError::Execution(format!("{e}")))
        })?;
        Ok(Box::new(RustAccumulator::new(
            accum,
            args.return_type().clone(),
        )))
    })
}

fn aggregate_udf_from_capsule(capsule: &Bound<'_, PyCapsule>) -> PyDataFusionResult<AggregateUDF> {
    validate_pycapsule(capsule, "datafusion_aggregate_udf")?;

    let udaf = unsafe { capsule.reference::<FFI_AggregateUDF>() };
    let udaf: ForeignAggregateUDF = udaf.try_into()?;

    Ok(udaf.into())
}

/// Represents an AggregateUDF
#[pyclass(frozen, name = "AggregateUDF", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyAggregateUDF {
    pub(crate) function: AggregateUDF,
}

#[pymethods]
impl PyAggregateUDF {
    #[new]
    #[pyo3(signature=(name, accumulator, input_type, return_type, state_type, volatility))]
    fn new(
        name: &str,
        accumulator: Py<PyAny>,
        input_type: PyArrowType<Vec<DataType>>,
        return_type: PyArrowType<DataType>,
        state_type: PyArrowType<Vec<DataType>>,
        volatility: &str,
    ) -> PyResult<Self> {
        let function = create_udaf(
            name,
            input_type.0,
            Arc::new(return_type.0),
            parse_volatility(volatility)?,
            to_rust_accumulator(accumulator),
            Arc::new(state_type.0),
        );
        Ok(Self { function })
    }

    #[staticmethod]
    pub fn from_pycapsule(func: Bound<'_, PyAny>) -> PyDataFusionResult<Self> {
        if func.is_instance_of::<PyCapsule>() {
            let capsule = func.downcast::<PyCapsule>().map_err(py_datafusion_err)?;
            let function = aggregate_udf_from_capsule(capsule)?;
            return Ok(Self { function });
        }

        if func.hasattr("__datafusion_aggregate_udf__")? {
            let capsule = func.getattr("__datafusion_aggregate_udf__")?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>().map_err(py_datafusion_err)?;
            let function = aggregate_udf_from_capsule(capsule)?;
            return Ok(Self { function });
        }

        Err(crate::errors::PyDataFusionError::Common(
            "__datafusion_aggregate_udf__ does not exist on AggregateUDF object.".to_string(),
        ))
    }

    /// creates a new PyExpr with the call of the udf
    #[pyo3(signature = (*args))]
    fn __call__(&self, args: Vec<PyExpr>) -> PyResult<PyExpr> {
        let args = args.iter().map(|e| e.expr.clone()).collect();
        Ok(self.function.call(args).into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("AggregateUDF({})", self.function.name()))
    }
}
