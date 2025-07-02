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

use datafusion::common::TableReference;
use datafusion::logical_expr::logical_plan::TableScan;
use pyo3::{prelude::*, IntoPyObjectExt};
use std::fmt::{self, Display, Formatter};

use crate::expr::logical_node::LogicalNode;
use crate::sql::logical::PyLogicalPlan;
use crate::{common::df_schema::PyDFSchema, expr::PyExpr};

#[pyclass(name = "TableScan", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyTableScan {
    table_scan: TableScan,
}

impl PyTableScan {
    pub fn new(table_scan: TableScan) -> Self {
        Self { table_scan }
    }
}

impl From<PyTableScan> for TableScan {
    fn from(tbl_scan: PyTableScan) -> TableScan {
        tbl_scan.table_scan
    }
}

impl From<TableScan> for PyTableScan {
    fn from(table_scan: TableScan) -> PyTableScan {
        PyTableScan { table_scan }
    }
}

impl Display for PyTableScan {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "TableScan\nTable Name: {}
            Projections: {:?}
            Projected Schema: {:?}
            Filters: {:?}",
            &self.table_scan.table_name,
            &self.py_projections(),
            &self.py_schema(),
            &self.py_filters(),
        )
    }
}

#[pymethods]
impl PyTableScan {
    /// Retrieves the name of the table represented by this `TableScan` instance
    #[pyo3(name = "table_name")]
    fn py_table_name(&self) -> PyResult<String> {
        Ok(format!("{}", self.table_scan.table_name))
    }

    #[pyo3(name = "fqn")]
    fn fqn(&self) -> PyResult<(Option<String>, Option<String>, String)> {
        let table_ref: TableReference = self.table_scan.table_name.clone();
        Ok(match table_ref {
            TableReference::Bare { table } => (None, None, table.to_string()),
            TableReference::Partial { schema, table } => {
                (None, Some(schema.to_string()), table.to_string())
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => (
                Some(catalog.to_string()),
                Some(schema.to_string()),
                table.to_string(),
            ),
        })
    }

    /// The column indexes that should be. Note if this is empty then
    /// all columns should be read by the `TableProvider`. This function
    /// provides a Tuple of the (index, column_name) to make things simpler
    /// for the calling code since often times the name is preferred to
    /// the index which is a lower level abstraction.
    #[pyo3(name = "projection")]
    fn py_projections(&self) -> PyResult<Vec<(usize, String)>> {
        match &self.table_scan.projection {
            Some(indices) => {
                let schema = self.table_scan.source.schema();
                Ok(indices
                    .iter()
                    .map(|i| (*i, schema.field(*i).name().to_string()))
                    .collect())
            }
            None => Ok(vec![]),
        }
    }

    /// Resulting schema from the `TableScan` operation
    #[pyo3(name = "schema")]
    fn py_schema(&self) -> PyResult<PyDFSchema> {
        Ok((*self.table_scan.projected_schema).clone().into())
    }

    /// Certain `TableProvider` physical readers offer the capability to filter rows that
    /// are read at read time. These `filters` are contained here.
    #[pyo3(name = "filters")]
    fn py_filters(&self) -> PyResult<Vec<PyExpr>> {
        Ok(self
            .table_scan
            .filters
            .iter()
            .map(|expr| PyExpr::from(expr.clone()))
            .collect())
    }

    /// Optional number of rows that should be read at read time by the `TableProvider`
    #[pyo3(name = "fetch")]
    fn py_fetch(&self) -> PyResult<Option<usize>> {
        Ok(self.table_scan.fetch)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("TableScan({self})"))
    }
}

impl LogicalNode for PyTableScan {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        // table scans are leaf nodes and do not have inputs
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
