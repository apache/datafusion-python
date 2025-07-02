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

use crate::{common::schema::PyConstraints, expr::PyExpr, sql::logical::PyLogicalPlan};
use std::{
    collections::HashMap,
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use datafusion::logical_expr::CreateExternalTable;
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::common::df_schema::PyDFSchema;

use super::{logical_node::LogicalNode, sort_expr::PySortExpr};

#[pyclass(name = "CreateExternalTable", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyCreateExternalTable {
    create: CreateExternalTable,
}

impl From<PyCreateExternalTable> for CreateExternalTable {
    fn from(create: PyCreateExternalTable) -> Self {
        create.create
    }
}

impl From<CreateExternalTable> for PyCreateExternalTable {
    fn from(create: CreateExternalTable) -> PyCreateExternalTable {
        PyCreateExternalTable { create }
    }
}

impl Display for PyCreateExternalTable {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "CreateExternalTable: {:?}{}",
            self.create.name, self.create.constraints
        )
    }
}

#[pymethods]
impl PyCreateExternalTable {
    #[allow(clippy::too_many_arguments)]
    #[new]
    #[pyo3(signature = (schema, name, location, file_type, table_partition_cols, if_not_exists, temporary, order_exprs, unbounded, options, constraints, column_defaults, definition=None))]
    pub fn new(
        schema: PyDFSchema,
        name: String,
        location: String,
        file_type: String,
        table_partition_cols: Vec<String>,
        if_not_exists: bool,
        temporary: bool,
        order_exprs: Vec<Vec<PySortExpr>>,
        unbounded: bool,
        options: HashMap<String, String>,
        constraints: PyConstraints,
        column_defaults: HashMap<String, PyExpr>,
        definition: Option<String>,
    ) -> Self {
        let create = CreateExternalTable {
            schema: Arc::new(schema.into()),
            name: name.into(),
            location,
            file_type,
            table_partition_cols,
            if_not_exists,
            temporary,
            definition,
            order_exprs: order_exprs
                .into_iter()
                .map(|vec| vec.into_iter().map(|s| s.into()).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            unbounded,
            options,
            constraints: constraints.constraints,
            column_defaults: column_defaults
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        };
        PyCreateExternalTable { create }
    }

    pub fn schema(&self) -> PyDFSchema {
        (*self.create.schema).clone().into()
    }

    pub fn name(&self) -> PyResult<String> {
        Ok(self.create.name.to_string())
    }

    pub fn location(&self) -> String {
        self.create.location.clone()
    }

    pub fn file_type(&self) -> String {
        self.create.file_type.clone()
    }

    pub fn table_partition_cols(&self) -> Vec<String> {
        self.create.table_partition_cols.clone()
    }

    pub fn if_not_exists(&self) -> bool {
        self.create.if_not_exists
    }

    pub fn temporary(&self) -> bool {
        self.create.temporary
    }

    pub fn definition(&self) -> Option<String> {
        self.create.definition.clone()
    }

    pub fn order_exprs(&self) -> Vec<Vec<PySortExpr>> {
        self.create
            .order_exprs
            .iter()
            .map(|vec| vec.iter().map(|s| s.clone().into()).collect())
            .collect()
    }

    pub fn unbounded(&self) -> bool {
        self.create.unbounded
    }

    pub fn options(&self) -> HashMap<String, String> {
        self.create.options.clone()
    }

    pub fn constraints(&self) -> PyConstraints {
        PyConstraints {
            constraints: self.create.constraints.clone(),
        }
    }

    pub fn column_defaults(&self) -> HashMap<String, PyExpr> {
        self.create
            .column_defaults
            .iter()
            .map(|(k, v)| (k.clone(), v.clone().into()))
            .collect()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("CreateExternalTable({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("CreateExternalTable".to_string())
    }
}

impl LogicalNode for PyCreateExternalTable {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
