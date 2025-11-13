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
use std::borrow::Cow;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::pyarrow::PyArrowType;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Constraints;
use datafusion::datasource::TableType;
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource};
use parking_lot::RwLock;
use pyo3::prelude::*;

use super::data_type::DataTypeMap;
use super::function::SqlFunction;
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "SqlSchema", module = "datafusion.common", subclass, frozen)]
#[derive(Debug, Clone)]
pub struct SqlSchema {
    name: Arc<RwLock<String>>,
    tables: Arc<RwLock<Vec<SqlTable>>>,
    views: Arc<RwLock<Vec<SqlView>>>,
    functions: Arc<RwLock<Vec<SqlFunction>>>,
}

#[pyclass(name = "SqlTable", module = "datafusion.common", subclass)]
#[derive(Debug, Clone)]
pub struct SqlTable {
    #[pyo3(get, set)]
    pub name: String,
    #[pyo3(get, set)]
    pub columns: Vec<(String, DataTypeMap)>,
    #[pyo3(get, set)]
    pub primary_key: Option<String>,
    #[pyo3(get, set)]
    pub foreign_keys: Vec<String>,
    #[pyo3(get, set)]
    pub indexes: Vec<String>,
    #[pyo3(get, set)]
    pub constraints: Vec<String>,
    #[pyo3(get, set)]
    pub statistics: SqlStatistics,
    #[pyo3(get, set)]
    pub filepaths: Option<Vec<String>>,
}

#[pymethods]
impl SqlTable {
    #[new]
    #[pyo3(signature = (table_name, columns, row_count, filepaths=None))]
    pub fn new(
        table_name: String,
        columns: Vec<(String, DataTypeMap)>,
        row_count: f64,
        filepaths: Option<Vec<String>>,
    ) -> Self {
        Self {
            name: table_name,
            columns,
            primary_key: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            constraints: Vec::new(),
            statistics: SqlStatistics::new(row_count),
            filepaths,
        }
    }
}

#[pyclass(name = "SqlView", module = "datafusion.common", subclass)]
#[derive(Debug, Clone)]
pub struct SqlView {
    #[pyo3(get, set)]
    pub name: String,
    #[pyo3(get, set)]
    pub definition: String, // SQL code that defines the view
}

#[pymethods]
impl SqlSchema {
    #[new]
    pub fn new(schema_name: &str) -> Self {
        Self {
            name: Arc::new(RwLock::new(schema_name.to_owned())),
            tables: Arc::new(RwLock::new(Vec::new())),
            views: Arc::new(RwLock::new(Vec::new())),
            functions: Arc::new(RwLock::new(Vec::new())),
        }
    }

    #[getter]
    fn name(&self) -> PyResult<String> {
        Ok(self.name.read().clone())
    }

    #[setter]
    fn set_name(&self, value: String) -> PyResult<()> {
        *self.name.write() = value;
        Ok(())
    }

    #[getter]
    fn tables(&self) -> PyResult<Vec<SqlTable>> {
        Ok(self.tables.read().clone())
    }

    #[setter]
    fn set_tables(&self, tables: Vec<SqlTable>) -> PyResult<()> {
        *self.tables.write() = tables;
        Ok(())
    }

    #[getter]
    fn views(&self) -> PyResult<Vec<SqlView>> {
        Ok(self.views.read().clone())
    }

    #[setter]
    fn set_views(&self, views: Vec<SqlView>) -> PyResult<()> {
        *self.views.write() = views;
        Ok(())
    }

    #[getter]
    fn functions(&self) -> PyResult<Vec<SqlFunction>> {
        Ok(self.functions.read().clone())
    }

    #[setter]
    fn set_functions(&self, functions: Vec<SqlFunction>) -> PyResult<()> {
        *self.functions.write() = functions;
        Ok(())
    }

    pub fn table_by_name(&self, table_name: &str) -> Option<SqlTable> {
        let tables = self.tables.read();
        tables.iter().find(|tbl| tbl.name.eq(table_name)).cloned()
    }

    pub fn add_table(&self, table: SqlTable) {
        let mut tables = self.tables.write();
        tables.push(table);
    }

    pub fn drop_table(&self, table_name: String) {
        let mut tables = self.tables.write();
        tables.retain(|x| !x.name.eq(&table_name));
    }
}

/// SqlTable wrapper that is compatible with DataFusion logical query plans
pub struct SqlTableSource {
    schema: SchemaRef,
    statistics: Option<SqlStatistics>,
    filepaths: Option<Vec<String>>,
}

impl SqlTableSource {
    /// Initialize a new `EmptyTable` from a schema
    pub fn new(
        schema: SchemaRef,
        statistics: Option<SqlStatistics>,
        filepaths: Option<Vec<String>>,
    ) -> Self {
        Self {
            schema,
            statistics,
            filepaths,
        }
    }

    /// Access optional statistics associated with this table source
    pub fn statistics(&self) -> Option<&SqlStatistics> {
        self.statistics.as_ref()
    }

    /// Access optional filepath associated with this table source
    #[allow(dead_code)]
    pub fn filepaths(&self) -> Option<&Vec<String>> {
        self.filepaths.as_ref()
    }
}

/// Implement TableSource, used in the logical query plan and in logical query optimizations
impl TableSource for SqlTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|f| {
                let filters = split_conjunction(f);
                if filters.iter().all(|f| is_supported_push_down_expr(f)) {
                    // Push down filters to the tablescan operation if all are supported
                    Ok(TableProviderFilterPushDown::Exact)
                } else if filters.iter().any(|f| is_supported_push_down_expr(f)) {
                    // Partially apply the filter in the TableScan but retain
                    // the Filter operator in the plan as well
                    Ok(TableProviderFilterPushDown::Inexact)
                } else {
                    Ok(TableProviderFilterPushDown::Unsupported)
                }
            })
            .collect()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, datafusion::logical_expr::LogicalPlan>> {
        None
    }
}

fn is_supported_push_down_expr(_expr: &Expr) -> bool {
    // For now we support all kinds of expr's at this level
    true
}

#[pyclass(frozen, name = "SqlStatistics", module = "datafusion.common", subclass)]
#[derive(Debug, Clone)]
pub struct SqlStatistics {
    row_count: f64,
}

#[pymethods]
impl SqlStatistics {
    #[new]
    pub fn new(row_count: f64) -> Self {
        Self { row_count }
    }

    #[pyo3(name = "getRowCount")]
    pub fn get_row_count(&self) -> f64 {
        self.row_count
    }
}

#[pyclass(frozen, name = "Constraints", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyConstraints {
    pub constraints: Constraints,
}

impl From<PyConstraints> for Constraints {
    fn from(constraints: PyConstraints) -> Self {
        constraints.constraints
    }
}

impl From<Constraints> for PyConstraints {
    fn from(constraints: Constraints) -> Self {
        PyConstraints { constraints }
    }
}

impl Display for PyConstraints {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Constraints: {:?}", self.constraints)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(frozen, eq, eq_int, name = "TableType", module = "datafusion.common")]
pub enum PyTableType {
    Base,
    View,
    Temporary,
}

impl From<PyTableType> for datafusion::logical_expr::TableType {
    fn from(table_type: PyTableType) -> Self {
        match table_type {
            PyTableType::Base => datafusion::logical_expr::TableType::Base,
            PyTableType::View => datafusion::logical_expr::TableType::View,
            PyTableType::Temporary => datafusion::logical_expr::TableType::Temporary,
        }
    }
}

impl From<TableType> for PyTableType {
    fn from(table_type: TableType) -> Self {
        match table_type {
            datafusion::logical_expr::TableType::Base => PyTableType::Base,
            datafusion::logical_expr::TableType::View => PyTableType::View,
            datafusion::logical_expr::TableType::Temporary => PyTableType::Temporary,
        }
    }
}

#[pyclass(frozen, name = "TableSource", module = "datafusion.common", subclass)]
#[derive(Clone)]
pub struct PyTableSource {
    pub table_source: Arc<dyn TableSource>,
}

#[pymethods]
impl PyTableSource {
    pub fn schema(&self) -> PyArrowType<Schema> {
        (*self.table_source.schema()).clone().into()
    }

    pub fn constraints(&self) -> Option<PyConstraints> {
        self.table_source.constraints().map(|c| PyConstraints {
            constraints: c.clone(),
        })
    }

    pub fn table_type(&self) -> PyTableType {
        self.table_source.table_type().into()
    }

    pub fn get_logical_plan(&self) -> Option<PyLogicalPlan> {
        self.table_source
            .get_logical_plan()
            .map(|plan| PyLogicalPlan::new(plan.into_owned()))
    }
}
