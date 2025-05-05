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

use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{DmlStatement, WriteOp};
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::common::schema::PyTableSource;
use crate::{common::df_schema::PyDFSchema, sql::logical::PyLogicalPlan};

use super::logical_node::LogicalNode;

#[pyclass(name = "DmlStatement", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyDmlStatement {
    dml: DmlStatement,
}

impl From<PyDmlStatement> for DmlStatement {
    fn from(dml: PyDmlStatement) -> Self {
        dml.dml
    }
}

impl From<DmlStatement> for PyDmlStatement {
    fn from(dml: DmlStatement) -> PyDmlStatement {
        PyDmlStatement { dml }
    }
}

impl LogicalNode for PyDmlStatement {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.dml.input).clone())]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}

#[pymethods]
impl PyDmlStatement {
    pub fn table_name(&self) -> PyResult<String> {
        Ok(self.dml.table_name.to_string())
    }

    pub fn target(&self) -> PyResult<PyTableSource> {
        Ok(PyTableSource {
            table_source: self.dml.target.clone(),
        })
    }

    pub fn op(&self) -> PyWriteOp {
        self.dml.op.clone().into()
    }

    pub fn input(&self) -> PyLogicalPlan {
        PyLogicalPlan {
            plan: self.dml.input.clone(),
        }
    }

    pub fn output_schema(&self) -> PyDFSchema {
        (*self.dml.output_schema).clone().into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok("DmlStatement".to_string())
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("DmlStatement".to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(eq, eq_int, name = "WriteOp", module = "datafusion.expr")]
pub enum PyWriteOp {
    Append,
    Overwrite,
    Replace,

    Update,
    Delete,
    Ctas,
}

impl From<WriteOp> for PyWriteOp {
    fn from(write_op: WriteOp) -> Self {
        match write_op {
            WriteOp::Insert(InsertOp::Append) => PyWriteOp::Append,
            WriteOp::Insert(InsertOp::Overwrite) => PyWriteOp::Overwrite,
            WriteOp::Insert(InsertOp::Replace) => PyWriteOp::Replace,

            WriteOp::Update => PyWriteOp::Update,
            WriteOp::Delete => PyWriteOp::Delete,
            WriteOp::Ctas => PyWriteOp::Ctas,
        }
    }
}

impl From<PyWriteOp> for WriteOp {
    fn from(py: PyWriteOp) -> Self {
        match py {
            PyWriteOp::Append => WriteOp::Insert(InsertOp::Append),
            PyWriteOp::Overwrite => WriteOp::Insert(InsertOp::Overwrite),
            PyWriteOp::Replace => WriteOp::Insert(InsertOp::Replace),

            PyWriteOp::Update => WriteOp::Update,
            PyWriteOp::Delete => WriteOp::Delete,
            PyWriteOp::Ctas => WriteOp::Ctas,
        }
    }
}

#[pymethods]
impl PyWriteOp {
    fn name(&self) -> String {
        let write_op: WriteOp = self.clone().into();
        write_op.name().to_string()
    }
}
