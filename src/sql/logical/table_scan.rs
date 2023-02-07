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

use datafusion_common::DFSchema;
use datafusion_expr::{logical_plan::TableScan, LogicalPlan};
use pyo3::prelude::*;

use crate::{
    expression::{py_expr_list, PyExpr},
    sql::exceptions::py_type_err,
};

#[pyclass(name = "TableScan", module = "dask_planner", subclass)]
#[derive(Clone, FromPyObject)]
pub struct PyTableScan {
    pub(crate) table_scan: TableScan,
    input: Arc<LogicalPlan>,
}

#[pymethods]
impl PyTableScan {
    #[pyo3(name = "getTableScanProjects")]
    fn scan_projects(&mut self) -> PyResult<Vec<String>> {
        match &self.table_scan.projection {
            Some(indices) => {
                let schema = self.table_scan.source.schema();
                Ok(indices
                    .iter()
                    .map(|i| schema.field(*i).name().to_string())
                    .collect())
            }
            None => Ok(vec![]),
        }
    }

    /// If the 'TableScan' contains columns that should be projected during the
    /// read return True, otherwise return False
    #[pyo3(name = "containsProjections")]
    fn contains_projections(&self) -> bool {
        self.table_scan.projection.is_some()
    }

    #[pyo3(name = "getFilters")]
    fn scan_filters(&self) -> PyResult<Vec<PyExpr>> {
        py_expr_list(&self.input, &self.table_scan.filters)
    }
}

// impl TryFrom<LogicalPlan> for PyTableScan {
//     type Error = PyErr;

//     fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
//         match logical_plan {
//             LogicalPlan::TableScan(table_scan) => {
//                 // Create an input logical plan that's identical to the table scan with schema from the table source
//                 let mut input = table_scan.clone();
//                 input.projected_schema = DFSchema::try_from_qualified_schema(
//                     &table_scan.table_name,
//                     &table_scan.source.schema(),
//                 )
//                 .map_or(input.projected_schema, Arc::new);

//                 Ok(PyTableScan {
//                     table_scan,
//                     input: Arc::new(LogicalPlan::TableScan(input)),
//                 })
//             }
//             _ => Err(py_type_err("unexpected plan")),
//         }
//     }
// }
