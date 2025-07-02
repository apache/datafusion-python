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

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayData, ArrayRef};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::window_state::WindowAggState;
use datafusion::scalar::ScalarValue;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::common::data_type::PyScalarValue;
use crate::errors::{py_datafusion_err, to_datafusion_err, PyDataFusionResult};
use crate::expr::PyExpr;
use crate::utils::{parse_volatility, validate_pycapsule};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::{FromPyArrow, PyArrowType, ToPyArrow};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    PartitionEvaluator, PartitionEvaluatorFactory, Signature, Volatility, WindowUDF, WindowUDFImpl,
};
use datafusion_ffi::udwf::{FFI_WindowUDF, ForeignWindowUDF};
use pyo3::types::{PyCapsule, PyList, PyTuple};

#[derive(Debug)]
struct RustPartitionEvaluator {
    evaluator: PyObject,
}

impl RustPartitionEvaluator {
    fn new(evaluator: PyObject) -> Self {
        Self { evaluator }
    }
}

impl PartitionEvaluator for RustPartitionEvaluator {
    fn memoize(&mut self, _state: &mut WindowAggState) -> Result<()> {
        Python::with_gil(|py| self.evaluator.bind(py).call_method0("memoize").map(|_| ()))
            .map_err(|e| DataFusionError::Execution(format!("{e}")))
    }

    fn get_range(&self, idx: usize, n_rows: usize) -> Result<Range<usize>> {
        Python::with_gil(|py| {
            let py_args = vec![idx.into_pyobject(py)?, n_rows.into_pyobject(py)?];
            let py_args = PyTuple::new(py, py_args)?;

            self.evaluator
                .bind(py)
                .call_method1("get_range", py_args)
                .and_then(|v| {
                    let tuple: Bound<'_, PyTuple> = v.extract()?;
                    if tuple.len() != 2 {
                        return Err(PyValueError::new_err(format!(
                            "Expected get_range to return tuple of length 2. Received length {}",
                            tuple.len()
                        )));
                    }

                    let start: usize = tuple.get_item(0).unwrap().extract()?;
                    let end: usize = tuple.get_item(1).unwrap().extract()?;

                    Ok(Range { start, end })
                })
        })
        .map_err(|e| DataFusionError::Execution(format!("{e}")))
    }

    fn is_causal(&self) -> bool {
        Python::with_gil(|py| {
            self.evaluator
                .bind(py)
                .call_method0("is_causal")
                .and_then(|v| v.extract())
                .unwrap_or(false)
        })
    }

    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        println!("evaluate all called with number of values {}", values.len());
        Python::with_gil(|py| {
            let py_values = PyList::new(
                py,
                values
                    .iter()
                    .map(|arg| arg.into_data().to_pyarrow(py).unwrap()),
            )?;
            let py_num_rows = num_rows.into_pyobject(py)?;
            let py_args = PyTuple::new(py, vec![py_values.as_any(), &py_num_rows])?;

            self.evaluator
                .bind(py)
                .call_method1("evaluate_all", py_args)
                .map(|v| {
                    let array_data = ArrayData::from_pyarrow_bound(&v).unwrap();
                    make_array(array_data)
                })
        })
        .map_err(to_datafusion_err)
    }

    fn evaluate(&mut self, values: &[ArrayRef], range: &Range<usize>) -> Result<ScalarValue> {
        Python::with_gil(|py| {
            let py_values = PyList::new(
                py,
                values
                    .iter()
                    .map(|arg| arg.into_data().to_pyarrow(py).unwrap()),
            )?;
            let range_tuple = PyTuple::new(py, vec![range.start, range.end])?;
            let py_args = PyTuple::new(py, vec![py_values.as_any(), range_tuple.as_any()])?;

            self.evaluator
                .bind(py)
                .call_method1("evaluate", py_args)
                .and_then(|v| v.extract::<PyScalarValue>())
                .map(|v| v.0)
        })
        .map_err(to_datafusion_err)
    }

    fn evaluate_all_with_rank(
        &self,
        num_rows: usize,
        ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        Python::with_gil(|py| {
            let ranks = ranks_in_partition
                .iter()
                .map(|r| PyTuple::new(py, vec![r.start, r.end]))
                .collect::<PyResult<Vec<_>>>()?;

            // 1. cast args to Pyarrow array
            let py_args = vec![
                num_rows.into_pyobject(py)?.into_any(),
                PyList::new(py, ranks)?.into_any(),
            ];

            let py_args = PyTuple::new(py, py_args)?;

            // 2. call function
            self.evaluator
                .bind(py)
                .call_method1("evaluate_all_with_rank", py_args)
                .map(|v| {
                    let array_data = ArrayData::from_pyarrow_bound(&v).unwrap();
                    make_array(array_data)
                })
        })
        .map_err(to_datafusion_err)
    }

    fn supports_bounded_execution(&self) -> bool {
        Python::with_gil(|py| {
            self.evaluator
                .bind(py)
                .call_method0("supports_bounded_execution")
                .and_then(|v| v.extract())
                .unwrap_or(false)
        })
    }

    fn uses_window_frame(&self) -> bool {
        Python::with_gil(|py| {
            self.evaluator
                .bind(py)
                .call_method0("uses_window_frame")
                .and_then(|v| v.extract())
                .unwrap_or(false)
        })
    }

    fn include_rank(&self) -> bool {
        Python::with_gil(|py| {
            self.evaluator
                .bind(py)
                .call_method0("include_rank")
                .and_then(|v| v.extract())
                .unwrap_or(false)
        })
    }
}

pub fn to_rust_partition_evaluator(evaluator: PyObject) -> PartitionEvaluatorFactory {
    Arc::new(move || -> Result<Box<dyn PartitionEvaluator>> {
        let evaluator = Python::with_gil(|py| {
            evaluator
                .call0(py)
                .map_err(|e| DataFusionError::Execution(e.to_string()))
        })?;
        Ok(Box::new(RustPartitionEvaluator::new(evaluator)))
    })
}

/// Represents an WindowUDF
#[pyclass(name = "WindowUDF", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyWindowUDF {
    pub(crate) function: WindowUDF,
}

#[pymethods]
impl PyWindowUDF {
    #[new]
    #[pyo3(signature=(name, evaluator, input_types, return_type, volatility))]
    fn new(
        name: &str,
        evaluator: PyObject,
        input_types: Vec<PyArrowType<DataType>>,
        return_type: PyArrowType<DataType>,
        volatility: &str,
    ) -> PyResult<Self> {
        let return_type = return_type.0;
        let input_types = input_types.into_iter().map(|t| t.0).collect();

        let function = WindowUDF::from(MultiColumnWindowUDF::new(
            name,
            input_types,
            return_type,
            parse_volatility(volatility)?,
            to_rust_partition_evaluator(evaluator),
        ));
        Ok(Self { function })
    }

    /// creates a new PyExpr with the call of the udf
    #[pyo3(signature = (*args))]
    fn __call__(&self, args: Vec<PyExpr>) -> PyResult<PyExpr> {
        let args = args.iter().map(|e| e.expr.clone()).collect();
        Ok(self.function.call(args).into())
    }

    #[staticmethod]
    pub fn from_pycapsule(func: Bound<'_, PyAny>) -> PyDataFusionResult<Self> {
        if func.hasattr("__datafusion_window_udf__")? {
            let capsule = func.getattr("__datafusion_window_udf__")?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>().map_err(py_datafusion_err)?;
            validate_pycapsule(capsule, "datafusion_window_udf")?;

            let udwf = unsafe { capsule.reference::<FFI_WindowUDF>() };
            let udwf: ForeignWindowUDF = udwf.try_into()?;

            Ok(Self {
                function: udwf.into(),
            })
        } else {
            Err(crate::errors::PyDataFusionError::Common(
                "__datafusion_window_udf__ does not exist on WindowUDF object.".to_string(),
            ))
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("WindowUDF({})", self.function.name()))
    }
}

pub struct MultiColumnWindowUDF {
    name: String,
    signature: Signature,
    return_type: DataType,
    partition_evaluator_factory: PartitionEvaluatorFactory,
}

impl std::fmt::Debug for MultiColumnWindowUDF {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("WindowUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("return_type", &"<func>")
            .field("partition_evaluator_factory", &"<FUNC>")
            .finish()
    }
}

impl MultiColumnWindowUDF {
    pub fn new(
        name: impl Into<String>,
        input_types: Vec<DataType>,
        return_type: DataType,
        volatility: Volatility,
        partition_evaluator_factory: PartitionEvaluatorFactory,
    ) -> Self {
        let name = name.into();
        let signature = Signature::exact(input_types, volatility);
        Self {
            name,
            signature,
            return_type,
            partition_evaluator_factory,
        }
    }
}

impl WindowUDFImpl for MultiColumnWindowUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<arrow::datatypes::FieldRef> {
        // TODO: Should nullable always be `true`?
        Ok(arrow::datatypes::Field::new(field_args.name(), self.return_type.clone(), true).into())
    }

    // TODO: Enable passing partition_evaluator_args to python?
    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let _ = _partition_evaluator_args;
        (self.partition_evaluator_factory)()
    }
}
