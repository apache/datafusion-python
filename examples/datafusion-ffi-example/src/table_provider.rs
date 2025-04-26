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

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::catalog::MemTable;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion_ffi::table_provider::FFI_TableProvider;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyCapsule;
use pyo3::{pyclass, pymethods, Bound, PyResult, Python};
use std::sync::Arc;

/// In order to provide a test that demonstrates different sized record batches,
/// the first batch will have num_rows, the second batch num_rows+1, and so on.
#[pyclass(name = "MyTableProvider", module = "datafusion_ffi_example", subclass)]
#[derive(Clone)]
pub(crate) struct MyTableProvider {
    num_cols: usize,
    num_rows: usize,
    num_batches: usize,
}

fn create_record_batch(
    schema: &Arc<Schema>,
    num_cols: usize,
    start_value: i32,
    num_values: usize,
) -> DataFusionResult<RecordBatch> {
    let end_value = start_value + num_values as i32;
    let row_values: Vec<i32> = (start_value..end_value).collect();

    let columns: Vec<_> = (0..num_cols)
        .map(|_| Arc::new(arrow::array::Int32Array::from(row_values.clone())) as ArrayRef)
        .collect();

    RecordBatch::try_new(Arc::clone(schema), columns).map_err(DataFusionError::from)
}

impl MyTableProvider {
    pub fn create_table(&self) -> DataFusionResult<MemTable> {
        let fields: Vec<_> = (0..self.num_cols)
            .map(|idx| (b'A' + idx as u8) as char)
            .map(|col_name| Field::new(col_name, DataType::Int32, true))
            .collect();

        let schema = Arc::new(Schema::new(fields));

        let batches: DataFusionResult<Vec<_>> = (0..self.num_batches)
            .map(|batch_idx| {
                let start_value = batch_idx * self.num_rows;
                create_record_batch(
                    &schema,
                    self.num_cols,
                    start_value as i32,
                    self.num_rows + batch_idx,
                )
            })
            .collect();

        MemTable::try_new(schema, vec![batches?])
    }
}

#[pymethods]
impl MyTableProvider {
    #[new]
    pub fn new(num_cols: usize, num_rows: usize, num_batches: usize) -> Self {
        Self {
            num_cols,
            num_rows,
            num_batches,
        }
    }

    pub fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_table_provider".into();

        let provider = self
            .create_table()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let provider = FFI_TableProvider::new(Arc::new(provider), false, None);

        PyCapsule::new(py, provider, Some(name))
    }
}
