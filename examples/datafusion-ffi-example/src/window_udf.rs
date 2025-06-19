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

use arrow_schema::{DataType, FieldRef};
use datafusion::error::Result as DataFusionResult;
use datafusion::functions_window::rank::rank_udwf;
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{PartitionEvaluator, Signature, WindowUDF, WindowUDFImpl};
use datafusion_ffi::udwf::FFI_WindowUDF;
use pyo3::types::PyCapsule;
use pyo3::{pyclass, pymethods, Bound, PyResult, Python};
use std::any::Any;
use std::sync::Arc;

#[pyclass(name = "MyRankUDF", module = "datafusion_ffi_example", subclass)]
#[derive(Debug, Clone)]
pub(crate) struct MyRankUDF {
    inner: Arc<WindowUDF>,
}

#[pymethods]
impl MyRankUDF {
    #[new]
    fn new() -> Self {
        Self { inner: rank_udwf() }
    }

    fn __datafusion_window_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_window_udf".into();

        let func = Arc::new(WindowUDF::from(self.clone()));
        let provider = FFI_WindowUDF::from(func);

        PyCapsule::new(py, provider, Some(name))
    }
}

impl WindowUDFImpl for MyRankUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "my_custom_rank"
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> DataFusionResult<Box<dyn PartitionEvaluator>> {
        self.inner
            .inner()
            .partition_evaluator(partition_evaluator_args)
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DataFusionResult<FieldRef> {
        self.inner.inner().field(field_args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }
}
