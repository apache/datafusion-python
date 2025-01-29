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

use datafusion::{dataframe::DataFrameWriteOptions, logical_expr::dml::InsertOp};
use pyo3::{exceptions::PyValueError, FromPyObject, PyErr, PyResult};

use crate::expr::sort_expr::{to_sort_expressions, PySortExpr};

#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub struct PyDataFrameWriteOptions {
    insert_operation: Option<String>,
    single_file_output: Option<bool>,
    partition_by: Option<Vec<String>>,
    sort_by: Option<Vec<PySortExpr>>,
}

impl TryInto<DataFrameWriteOptions> for PyDataFrameWriteOptions {
    type Error = PyErr;

    fn try_into(self) -> PyResult<DataFrameWriteOptions> {
        let mut options = DataFrameWriteOptions::new();
        if let Some(insert_op) = self.insert_operation {
            let op = match insert_op.as_str() {
                "append" => InsertOp::Append,
                "overwrite" => InsertOp::Overwrite,
                "replace" => InsertOp::Replace,
                _ => {
                    return Err(PyValueError::new_err(format!(
                        "Unrecognized insert op {insert_op}"
                    )))
                }
            };
            options = options.with_insert_operation(op);
        }
        if let Some(single_file_output) = self.single_file_output {
            options = options.with_single_file_output(single_file_output);
        }

        if let Some(partition_by) = self.partition_by {
            options = options.with_partition_by(partition_by);
        }

        if let Some(sort_by) = self.sort_by {
            options = options.with_sort_by(to_sort_expressions(sort_by));
        }

        Ok(options)
    }
}

pub fn make_dataframe_write_options(
    write_options: Option<PyDataFrameWriteOptions>,
) -> PyResult<DataFrameWriteOptions> {
    if let Some(wo) = write_options {
        wo.try_into()
    } else {
        Ok(DataFrameWriteOptions::new())
    }
}
