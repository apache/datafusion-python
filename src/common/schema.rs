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

use datafusion::arrow::datatypes::SchemaRef;
use datafusion_expr::{TableSource, TableProviderFilterPushDown, Expr};
use pyo3::prelude::*;

use datafusion_optimizer::utils::split_conjunction;

use super::{function::SqlFunction, data_type::DataTypeMap};

#[pyclass(name = "SqlSchema", module = "datafusion.common", subclass)]
#[derive(Debug, Clone)]
pub struct SqlSchema {
    #[pyo3(get, set)]
    pub name: String,
    #[pyo3(get, set)]
    pub tables: Vec<SqlTable>,
    #[pyo3(get, set)]
    pub views: Vec<SqlView>,
    #[pyo3(get, set)]
    pub functions: Vec<SqlFunction>,
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
    pub filepath: Option<String>,
}

#[pymethods]
impl SqlTable {
    #[new]
    pub fn new(
        _schema_name: String,
        table_name: String,
        columns: Vec<(String, DataTypeMap)>,
        // primary_key: Option<String>,
        // foreign_keys: Vec<String>,
        // indexes: Vec<String>,
        // constraints: Vec<String>,
        row_count: f64,
        filepath: Option<String>,
    ) -> Self {
        Self {
            name: table_name,
            columns,
            primary_key: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            constraints: Vec::new(),
            statistics: SqlStatistics::new(row_count),
            filepath,
        }
    }

    // // TODO: Really wish we could accept a SqlTypeName instance here instead of a String for `column_type` ....
    // #[pyo3(name = "add_column")]
    // pub fn add_column(&mut self, column_name: &str, type_map: DaskTypeMap) {
    //     self.columns.push((column_name.to_owned(), type_map));
    // }

    // #[pyo3(name = "getSchema")]
    // pub fn get_schema(&self) -> PyResult<Option<String>> {
    //     Ok(self.schema_name.clone())
    // }

    // #[pyo3(name = "getTableName")]
    // pub fn get_table_name(&self) -> PyResult<String> {
    //     Ok(self.table_name.clone())
    // }

    // #[pyo3(name = "getQualifiedName")]
    // pub fn qualified_name(&self, plan: logical::PyLogicalPlan) -> Vec<String> {
    //     let mut qualified_name = match &self.schema_name {
    //         Some(schema_name) => vec![schema_name.clone()],
    //         None => vec![],
    //     };

    //     match plan.original_plan {
    //         LogicalPlan::TableScan(table_scan) => {
    //             qualified_name.push(table_scan.table_name.to_string());
    //         }
    //         _ => {
    //             qualified_name.push(self.table_name.clone());
    //         }
    //     }

    //     qualified_name
    // }

    // #[pyo3(name = "getRowType")]
    // pub fn row_type(&self) -> RelDataType {
    //     let mut fields: Vec<RelDataTypeField> = Vec::new();
    //     for (name, data_type) in &self.columns {
    //         fields.push(RelDataTypeField::new(name.as_str(), data_type.clone(), 255));
    //     }
    //     RelDataType::new(false, fields)
    // }
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
            name: schema_name.to_owned(),
            tables: Vec::new(),
            views: Vec::new(),
            functions: Vec::new(),
        }
    }

    pub fn table_by_name(&self, table_name: &str) -> Option<SqlTable> {
        for tbl in &self.tables {
            if tbl.name.eq(table_name) {
                return Some(tbl.clone());
            }
        }
        None
    }

    pub fn add_table(&mut self, table: SqlTable) {
        self.tables.push(table);
    }

    // pub fn add_or_overload_function(
    //     &mut self,
    //     name: String,
    //     input_types: Vec<PyDataType>,
    //     return_type: PyDataType,
    //     aggregation: bool,
    // ) {
    //     self.functions
    //         .entry(name.clone())
    //         .and_modify(|e| {
    //             (*e).lock()
    //                 .unwrap()
    //                 .add_type_mapping(input_types.clone(), return_type.clone());
    //         })
    //         .or_insert_with(|| {
    //             Arc::new(Mutex::new(SQLFunction::new(
    //                 name,
    //                 input_types,
    //                 return_type,
    //                 aggregation,
    //             )))
    //         });
    // }
}


/// SqlTable wrapper that is compatible with DataFusion logical query plans
pub struct SqlTableSource {
    schema: SchemaRef,
    statistics: Option<SqlStatistics>,
    filepath: Option<String>,
}

impl SqlTableSource {
    /// Initialize a new `EmptyTable` from a schema
    pub fn new(
        schema: SchemaRef,
        statistics: Option<SqlStatistics>,
        filepath: Option<String>,
    ) -> Self {
        Self {
            schema,
            statistics,
            filepath,
        }
    }

    /// Access optional statistics associated with this table source
    pub fn statistics(&self) -> Option<&SqlStatistics> {
        self.statistics.as_ref()
    }

    /// Access optional filepath associated with this table source
    #[allow(dead_code)]
    pub fn filepath(&self) -> Option<&String> {
        self.filepath.as_ref()
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

    fn supports_filter_pushdown(
        &self,
        filter: &Expr,
    ) -> datafusion_common::Result<TableProviderFilterPushDown> {
        let filters = split_conjunction(filter);
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
    }

    fn table_type(&self) -> datafusion_expr::TableType {
        datafusion_expr::TableType::Base
    }

    #[allow(deprecated)]
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|f| self.supports_filter_pushdown(f))
            .collect()
    }

    fn get_logical_plan(&self) -> Option<&datafusion_expr::LogicalPlan> {
        None
    }
}

fn is_supported_push_down_expr(_expr: &Expr) -> bool {
    // For now we support all kinds of expr's at this level
    true
}

#[pyclass(name = "SqlStatistics", module = "datafusion.common", subclass)]
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
