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

use arrow::datatypes::{DataType, Schema};
use arrow::pyarrow::PyArrowType;
use datafusion::prelude::CsvReadOptions;
use pyo3::prelude::{PyModule, PyModuleMethods};
use pyo3::{Bound, PyResult, pyclass, pymethods};

use crate::context::parse_file_compression_type;
use crate::errors::PyDataFusionError;
use crate::expr::sort_expr::PySortExpr;

/// Options for reading CSV files
#[pyclass(name = "CsvReadOptions", module = "datafusion.options", frozen)]
pub struct PyCsvReadOptions {
    pub has_header: bool,
    pub delimiter: u8,
    pub quote: u8,
    pub terminator: Option<u8>,
    pub escape: Option<u8>,
    pub comment: Option<u8>,
    pub newlines_in_values: bool,
    pub schema: Option<PyArrowType<Schema>>,
    pub schema_infer_max_records: usize,
    pub file_extension: String,
    pub table_partition_cols: Vec<(String, PyArrowType<DataType>)>,
    pub file_compression_type: String,
    pub file_sort_order: Vec<Vec<PySortExpr>>,
    pub null_regex: Option<String>,
    pub truncated_rows: bool,
}

#[pymethods]
impl PyCsvReadOptions {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        has_header=true,
        delimiter=b',',
        quote=b'"',
        terminator=None,
        escape=None,
        comment=None,
        newlines_in_values=false,
        schema=None,
        schema_infer_max_records=1000,
        file_extension=".csv".to_string(),
        table_partition_cols=vec![],
        file_compression_type="".to_string(),
        file_sort_order=vec![],
        null_regex=None,
        truncated_rows=false
    ))]
    #[new]
    fn new(
        has_header: bool,
        delimiter: u8,
        quote: u8,
        terminator: Option<u8>,
        escape: Option<u8>,
        comment: Option<u8>,
        newlines_in_values: bool,
        schema: Option<PyArrowType<Schema>>,
        schema_infer_max_records: usize,
        file_extension: String,
        table_partition_cols: Vec<(String, PyArrowType<DataType>)>,
        file_compression_type: String,
        file_sort_order: Vec<Vec<PySortExpr>>,
        null_regex: Option<String>,
        truncated_rows: bool,
    ) -> Self {
        Self {
            has_header,
            delimiter,
            quote,
            terminator,
            escape,
            comment,
            newlines_in_values,
            schema,
            schema_infer_max_records,
            file_extension,
            table_partition_cols,
            file_compression_type,
            file_sort_order,
            null_regex,
            truncated_rows,
        }
    }
}

impl<'a> TryFrom<&'a PyCsvReadOptions> for CsvReadOptions<'a> {
    type Error = PyDataFusionError;

    fn try_from(value: &'a PyCsvReadOptions) -> Result<CsvReadOptions<'a>, Self::Error> {
        let partition_cols: Vec<(String, DataType)> = value
            .table_partition_cols
            .iter()
            .map(|(name, dtype)| (name.clone(), dtype.0.clone()))
            .collect();

        let compression = parse_file_compression_type(Some(value.file_compression_type.clone()))?;

        let sort_order: Vec<Vec<datafusion::logical_expr::SortExpr>> = value
            .file_sort_order
            .iter()
            .map(|inner| {
                inner
                    .iter()
                    .map(|sort_expr| sort_expr.sort.clone())
                    .collect()
            })
            .collect();

        // Explicit struct initialization to catch upstream changes
        let mut options = CsvReadOptions {
            has_header: value.has_header,
            delimiter: value.delimiter,
            quote: value.quote,
            terminator: value.terminator,
            escape: value.escape,
            comment: value.comment,
            newlines_in_values: value.newlines_in_values,
            schema: None, // Will be set separately due to lifetime constraints
            schema_infer_max_records: value.schema_infer_max_records,
            file_extension: value.file_extension.as_str(),
            table_partition_cols: partition_cols,
            file_compression_type: compression,
            file_sort_order: sort_order,
            null_regex: value.null_regex.clone(),
            truncated_rows: value.truncated_rows,
        };

        // Set schema separately to handle the lifetime
        options.schema = value.schema.as_ref().map(|s| &s.0);

        Ok(options)
    }
}

pub(crate) fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCsvReadOptions>()?;

    Ok(())
}
