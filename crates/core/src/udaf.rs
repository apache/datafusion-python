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
use std::ptr::NonNull;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::arrow::pyarrow::{PyArrowType, ToPyArrow};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion_ffi::udaf::FFI_AggregateUDF;
use datafusion_python_util::parse_volatility;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};

use crate::common::data_type::PyScalarValue;
use crate::errors::{PyDataFusionResult, py_datafusion_err, to_datafusion_err};
use crate::expr::PyExpr;

#[derive(Debug)]
struct RustAccumulator {
    accum: Py<PyAny>,
}

impl RustAccumulator {
    fn new(accum: Py<PyAny>) -> Self {
        Self { accum }
    }
}

impl Accumulator for RustAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Python::attach(|py| -> PyResult<Vec<ScalarValue>> {
            let values = self.accum.bind(py).call_method0("state")?;
            let mut scalars = Vec::new();
            for item in values.try_iter()? {
                let item: Bound<'_, PyAny> = item?;
                let scalar = item.extract::<PyScalarValue>()?.0;
                scalars.push(scalar);
            }
            Ok(scalars)
        })
        .map_err(|e| DataFusionError::Execution(format!("{e}")))
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Python::attach(|py| -> PyResult<ScalarValue> {
            let value = self.accum.bind(py).call_method0("evaluate")?;
            value.extract::<PyScalarValue>().map(|v| v.0)
        })
        .map_err(|e| DataFusionError::Execution(format!("{e}")))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        Python::attach(|py| {
            // 1. cast args to Pyarrow array
            let py_args = values
                .iter()
                .map(|arg| arg.to_data().to_pyarrow(py).unwrap())
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
                        .to_data()
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
                .map(|arg| arg.to_data().to_pyarrow(py).unwrap())
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

fn instantiate_accumulator(accum: &Py<PyAny>) -> Result<Box<dyn Accumulator>> {
    let instance = Python::attach(|py| {
        accum
            .call0(py)
            .map_err(|e| DataFusionError::Execution(format!("{e}")))
    })?;
    Ok(Box::new(RustAccumulator::new(instance)))
}

/// Named-struct `AggregateUDFImpl` for Python-defined aggregate UDFs.
/// Holds the Python accumulator factory directly so the codec can
/// downcast and cloudpickle it across process boundaries.
#[derive(Debug)]
pub(crate) struct PythonFunctionAggregateUDF {
    name: String,
    accumulator: Py<PyAny>,
    signature: Signature,
    return_type: DataType,
    state_fields: Vec<FieldRef>,
}

impl PythonFunctionAggregateUDF {
    fn new(
        name: String,
        accumulator: Py<PyAny>,
        input_types: Vec<DataType>,
        return_type: DataType,
        state_types: Vec<DataType>,
        volatility: Volatility,
    ) -> Self {
        let signature = Signature::exact(input_types, volatility);
        let state_fields = state_types
            .into_iter()
            .enumerate()
            .map(|(i, t)| Arc::new(Field::new(format!("{i}"), t, true)))
            .collect();
        Self {
            name,
            accumulator,
            signature,
            return_type,
            state_fields,
        }
    }

    /// Stored Python callable that returns a fresh accumulator instance
    /// per partition. Consumed by the codec to cloudpickle the factory
    /// across process boundaries.
    pub(crate) fn accumulator(&self) -> &Py<PyAny> {
        &self.accumulator
    }

    pub(crate) fn return_type(&self) -> &DataType {
        &self.return_type
    }

    pub(crate) fn state_fields_ref(&self) -> &[FieldRef] {
        &self.state_fields
    }

    /// Reconstruct a `PythonFunctionAggregateUDF` from the parts emitted
    /// by the codec. `state_fields` carries the full state schema
    /// (names, data types, nullability, metadata) — the codec extracts
    /// it from the IPC payload, so the post-decode state schema is
    /// identical to the pre-encode one. Use [`Self::new`] when only
    /// `Vec<DataType>` is available (e.g. the Python constructor path,
    /// where field names are synthesized).
    pub(crate) fn from_parts(
        name: String,
        accumulator: Py<PyAny>,
        input_types: Vec<DataType>,
        return_type: DataType,
        state_fields: Vec<FieldRef>,
        volatility: Volatility,
    ) -> Self {
        Self {
            name,
            accumulator,
            signature: Signature::exact(input_types, volatility),
            return_type,
            state_fields,
        }
    }
}

impl Eq for PythonFunctionAggregateUDF {}
impl PartialEq for PythonFunctionAggregateUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.signature == other.signature
            && self.return_type == other.return_type
            && self.state_fields == other.state_fields
            && Python::attach(|py| {
                self.accumulator
                    .bind(py)
                    .eq(other.accumulator.bind(py))
                    .unwrap_or(false)
            })
    }
}

impl std::hash::Hash for PythonFunctionAggregateUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
        self.return_type.hash(state);
        for f in &self.state_fields {
            f.hash(state);
        }
        Python::attach(|py| {
            let py_hash = self.accumulator.bind(py).hash().unwrap_or(0);
            state.write_isize(py_hash);
        });
    }
}

impl AggregateUDFImpl for PythonFunctionAggregateUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        instantiate_accumulator(&self.accumulator)
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(self.state_fields.clone())
    }
}

fn aggregate_udf_from_capsule(capsule: &Bound<'_, PyCapsule>) -> PyDataFusionResult<AggregateUDF> {
    let data: NonNull<FFI_AggregateUDF> = capsule
        .pointer_checked(Some(c"datafusion_aggregate_udf"))?
        .cast();
    let udaf = unsafe { data.as_ref() };
    let udaf: Arc<dyn AggregateUDFImpl> = udaf.into();

    Ok(AggregateUDF::new_from_shared_impl(udaf))
}

/// Represents an AggregateUDF
#[pyclass(
    from_py_object,
    frozen,
    name = "AggregateUDF",
    module = "datafusion",
    subclass
)]
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
        let py_udf = PythonFunctionAggregateUDF::new(
            name.to_string(),
            accumulator,
            input_type.0,
            return_type.0,
            state_type.0,
            parse_volatility(volatility)?,
        );
        let function = AggregateUDF::new_from_impl(py_udf);
        Ok(Self { function })
    }

    #[staticmethod]
    pub fn from_pycapsule(func: Bound<'_, PyAny>) -> PyDataFusionResult<Self> {
        if func.is_instance_of::<PyCapsule>() {
            let capsule = func.cast::<PyCapsule>().map_err(py_datafusion_err)?;
            let function = aggregate_udf_from_capsule(capsule)?;
            return Ok(Self { function });
        }

        if func.hasattr("__datafusion_aggregate_udf__")? {
            let capsule = func.getattr("__datafusion_aggregate_udf__")?.call0()?;
            let capsule = capsule.cast::<PyCapsule>().map_err(py_datafusion_err)?;
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
