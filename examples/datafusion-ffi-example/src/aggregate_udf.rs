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

use arrow_schema::DataType;
use datafusion::error::Result as DataFusionResult;
use datafusion::functions_aggregate::sum::Sum;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature};
use datafusion_ffi::udaf::FFI_AggregateUDF;
use pyo3::types::PyCapsule;
use pyo3::{pyclass, pymethods, Bound, PyResult, Python};
use std::any::Any;
use std::sync::Arc;

#[pyclass(name = "MySumUDF", module = "datafusion_ffi_example", subclass)]
#[derive(Debug, Clone)]
pub(crate) struct MySumUDF {
    inner: Arc<Sum>,
}

#[pymethods]
impl MySumUDF {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(Sum::new()),
        }
    }

    fn __datafusion_aggregate_udf__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_aggregate_udf".into();

        let func = Arc::new(AggregateUDF::from(self.clone()));
        let provider = FFI_AggregateUDF::from(func);

        PyCapsule::new(py, provider, Some(name))
    }
}

impl AggregateUDFImpl for MySumUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "my_custom_sum"
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        self.inner.return_type(arg_types)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DataFusionResult<Box<dyn Accumulator>> {
        self.inner.accumulator(acc_args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }
}
