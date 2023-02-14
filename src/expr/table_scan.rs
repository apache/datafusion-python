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

use std::fmt::{self, Display, Formatter};
use datafusion_expr::logical_plan::TableScan;
use pyo3::prelude::*;

use crate::expr::PyExpr;



#[pyclass(name = "TableScan", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyTableScan {
    table_scan: TableScan,
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
        write!(f, "TableScan\nTable Name: {}
            \nProjections: {:?}
            \nProjected Schema: {:?}
            \nFilters: {:?}",
            &self.table_scan.table_name,
            &self.py_projections(),
            self.table_scan.projected_schema,
            self.py_filters(),
        )
    }
}

#[pymethods]
impl PyTableScan {

    /// Retrieves the name of the table represented by this `TableScan` instance
    #[pyo3(name = "table_name")]
    fn py_table_name(&self) -> PyResult<&str> {
        Ok(&self.table_scan.table_name)
    }

    /// TODO: Bindings for `TableSource` need to exist first. Left as a
    /// placeholder to display intention to add when able to.
    // #[pyo3(name = "source")]
    // fn py_source(&self) -> PyResult<Arc<dyn TableSource>> {
    //     Ok(self.table_scan.source)
    // }

    /// The column indexes that should be. Note if this is empty then 
    /// all columns should be read by the `TableProvider`. This function
    /// provides a Tuple of the (index, column_name) to make things simplier
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

    /// TODO: Bindings for `DFSchema` need to exist first. Left as a
    /// placeholder to display intention to add when able to.
    // /// Resulting schema from the `TableScan` operation
    // #[pyo3(name = "projectedSchema")]
    // fn py_projected_schema(&self) -> PyResult<DFSchemaRef> {
    //     Ok(self.table_scan.projected_schema)
    // }

    /// Certain `TableProvider` physical readers offer the capability to filter rows that
    /// are read at read time. These `filters` are contained here.
    #[pyo3(name = "filters")]
    fn py_filters(&self) -> PyResult<Vec<PyExpr>> {
        Ok(
            self.table_scan.filters
                .iter()
                .map(|expr| PyExpr::from(expr.clone()))
                .collect()
        )
    }

    /// Optional number of rows that should be read at read time by the `TableProvider`
    #[pyo3(name = "fetch")]
    fn py_fetch(&self) -> PyResult<Option<usize>> {
        Ok(self.table_scan.fetch)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("TableScan({})", self))
    }

}
