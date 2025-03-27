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

use datafusion::logical_expr::{
    Deallocate, Execute, Prepare, SetVariable, TransactionAccessMode, TransactionConclusion,
    TransactionEnd, TransactionIsolationLevel, TransactionStart,
};
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::{common::data_type::PyDataType, sql::logical::PyLogicalPlan};

use super::{logical_node::LogicalNode, PyExpr};

#[pyclass(name = "TransactionStart", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyTransactionStart {
    transaction_start: TransactionStart,
}

impl From<TransactionStart> for PyTransactionStart {
    fn from(transaction_start: TransactionStart) -> PyTransactionStart {
        PyTransactionStart { transaction_start }
    }
}

impl TryFrom<PyTransactionStart> for TransactionStart {
    type Error = PyErr;

    fn try_from(py: PyTransactionStart) -> Result<Self, Self::Error> {
        Ok(py.transaction_start)
    }
}

impl LogicalNode for PyTransactionStart {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(eq, eq_int, name = "TransactionAccessMode", module = "datafusion.expr")]
pub enum PyTransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

impl From<TransactionAccessMode> for PyTransactionAccessMode {
    fn from(access_mode: TransactionAccessMode) -> PyTransactionAccessMode {
        match access_mode {
            TransactionAccessMode::ReadOnly => PyTransactionAccessMode::ReadOnly,
            TransactionAccessMode::ReadWrite => PyTransactionAccessMode::ReadWrite,
        }
    }
}

impl TryFrom<PyTransactionAccessMode> for TransactionAccessMode {
    type Error = PyErr;

    fn try_from(py: PyTransactionAccessMode) -> Result<Self, Self::Error> {
        match py {
            PyTransactionAccessMode::ReadOnly => Ok(TransactionAccessMode::ReadOnly),
            PyTransactionAccessMode::ReadWrite => Ok(TransactionAccessMode::ReadWrite),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(
    eq,
    eq_int,
    name = "TransactionIsolationLevel",
    module = "datafusion.expr"
)]
pub enum PyTransactionIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
}

impl From<TransactionIsolationLevel> for PyTransactionIsolationLevel {
    fn from(isolation_level: TransactionIsolationLevel) -> PyTransactionIsolationLevel {
        match isolation_level {
            TransactionIsolationLevel::ReadUncommitted => {
                PyTransactionIsolationLevel::ReadUncommitted
            }
            TransactionIsolationLevel::ReadCommitted => PyTransactionIsolationLevel::ReadCommitted,
            TransactionIsolationLevel::RepeatableRead => {
                PyTransactionIsolationLevel::RepeatableRead
            }
            TransactionIsolationLevel::Serializable => PyTransactionIsolationLevel::Serializable,
            TransactionIsolationLevel::Snapshot => PyTransactionIsolationLevel::Snapshot,
        }
    }
}

impl TryFrom<PyTransactionIsolationLevel> for TransactionIsolationLevel {
    type Error = PyErr;

    fn try_from(value: PyTransactionIsolationLevel) -> Result<Self, Self::Error> {
        match value {
            PyTransactionIsolationLevel::ReadUncommitted => {
                Ok(TransactionIsolationLevel::ReadUncommitted)
            }
            PyTransactionIsolationLevel::ReadCommitted => {
                Ok(TransactionIsolationLevel::ReadCommitted)
            }
            PyTransactionIsolationLevel::RepeatableRead => {
                Ok(TransactionIsolationLevel::RepeatableRead)
            }
            PyTransactionIsolationLevel::Serializable => {
                Ok(TransactionIsolationLevel::Serializable)
            }
            PyTransactionIsolationLevel::Snapshot => Ok(TransactionIsolationLevel::Snapshot),
        }
    }
}

#[pymethods]
impl PyTransactionStart {
    #[new]
    pub fn new(
        access_mode: PyTransactionAccessMode,
        isolation_level: PyTransactionIsolationLevel,
    ) -> PyResult<Self> {
        let access_mode = access_mode.try_into()?;
        let isolation_level = isolation_level.try_into()?;
        Ok(PyTransactionStart {
            transaction_start: TransactionStart {
                access_mode,
                isolation_level,
            },
        })
    }

    pub fn access_mode(&self) -> PyResult<PyTransactionAccessMode> {
        Ok(self.transaction_start.access_mode.clone().into())
    }

    pub fn isolation_level(&self) -> PyResult<PyTransactionIsolationLevel> {
        Ok(self.transaction_start.isolation_level.clone().into())
    }
}

#[pyclass(name = "TransactionEnd", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyTransactionEnd {
    transaction_end: TransactionEnd,
}

impl From<TransactionEnd> for PyTransactionEnd {
    fn from(transaction_end: TransactionEnd) -> PyTransactionEnd {
        PyTransactionEnd { transaction_end }
    }
}

impl TryFrom<PyTransactionEnd> for TransactionEnd {
    type Error = PyErr;

    fn try_from(py: PyTransactionEnd) -> Result<Self, Self::Error> {
        Ok(py.transaction_end)
    }
}

impl LogicalNode for PyTransactionEnd {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(eq, eq_int, name = "TransactionConclusion", module = "datafusion.expr")]
pub enum PyTransactionConclusion {
    Commit,
    Rollback,
}

impl From<TransactionConclusion> for PyTransactionConclusion {
    fn from(value: TransactionConclusion) -> Self {
        match value {
            TransactionConclusion::Commit => PyTransactionConclusion::Commit,
            TransactionConclusion::Rollback => PyTransactionConclusion::Rollback,
        }
    }
}

impl TryFrom<PyTransactionConclusion> for TransactionConclusion {
    type Error = PyErr;

    fn try_from(value: PyTransactionConclusion) -> Result<Self, Self::Error> {
        match value {
            PyTransactionConclusion::Commit => Ok(TransactionConclusion::Commit),
            PyTransactionConclusion::Rollback => Ok(TransactionConclusion::Rollback),
        }
    }
}
#[pymethods]
impl PyTransactionEnd {
    #[new]
    pub fn new(conclusion: PyTransactionConclusion, chain: bool) -> PyResult<Self> {
        let conclusion = conclusion.try_into()?;
        Ok(PyTransactionEnd {
            transaction_end: TransactionEnd { conclusion, chain },
        })
    }

    pub fn conclusion(&self) -> PyResult<PyTransactionConclusion> {
        Ok(self.transaction_end.conclusion.clone().into())
    }

    pub fn chain(&self) -> bool {
        self.transaction_end.chain
    }
}

#[pyclass(name = "SetVariable", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PySetVariable {
    set_variable: SetVariable,
}

impl From<SetVariable> for PySetVariable {
    fn from(set_variable: SetVariable) -> PySetVariable {
        PySetVariable { set_variable }
    }
}

impl TryFrom<PySetVariable> for SetVariable {
    type Error = PyErr;

    fn try_from(py: PySetVariable) -> Result<Self, Self::Error> {
        Ok(py.set_variable)
    }
}

impl LogicalNode for PySetVariable {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}

#[pymethods]
impl PySetVariable {
    #[new]
    pub fn new(variable: String, value: String) -> Self {
        PySetVariable {
            set_variable: SetVariable { variable, value },
        }
    }

    pub fn variable(&self) -> String {
        self.set_variable.variable.clone()
    }

    pub fn value(&self) -> String {
        self.set_variable.value.clone()
    }
}

#[pyclass(name = "Prepare", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyPrepare {
    prepare: Prepare,
}

impl From<Prepare> for PyPrepare {
    fn from(prepare: Prepare) -> PyPrepare {
        PyPrepare { prepare }
    }
}

impl TryFrom<PyPrepare> for Prepare {
    type Error = PyErr;

    fn try_from(py: PyPrepare) -> Result<Self, Self::Error> {
        Ok(py.prepare)
    }
}

impl LogicalNode for PyPrepare {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.prepare.input).clone())]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}

#[pymethods]
impl PyPrepare {
    #[new]
    pub fn new(name: String, data_types: Vec<PyDataType>, input: PyLogicalPlan) -> Self {
        let input = input.plan().clone();
        let data_types = data_types
            .into_iter()
            .map(|data_type| data_type.into())
            .collect();
        PyPrepare {
            prepare: Prepare {
                name,
                data_types,
                input,
            },
        }
    }

    pub fn name(&self) -> String {
        self.prepare.name.clone()
    }

    pub fn data_types(&self) -> Vec<PyDataType> {
        self.prepare
            .data_types
            .clone()
            .into_iter()
            .map(|t| t.into())
            .collect()
    }

    pub fn input(&self) -> PyLogicalPlan {
        PyLogicalPlan {
            plan: self.prepare.input.clone(),
        }
    }
}

#[pyclass(name = "Execute", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyExecute {
    execute: Execute,
}

impl From<Execute> for PyExecute {
    fn from(execute: Execute) -> PyExecute {
        PyExecute { execute }
    }
}

impl TryFrom<PyExecute> for Execute {
    type Error = PyErr;

    fn try_from(py: PyExecute) -> Result<Self, Self::Error> {
        Ok(py.execute)
    }
}

impl LogicalNode for PyExecute {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}

#[pymethods]
impl PyExecute {
    #[new]
    pub fn new(name: String, parameters: Vec<PyExpr>) -> Self {
        let parameters = parameters
            .into_iter()
            .map(|parameter| parameter.into())
            .collect();
        PyExecute {
            execute: Execute { name, parameters },
        }
    }

    pub fn name(&self) -> String {
        self.execute.name.clone()
    }

    pub fn parameters(&self) -> Vec<PyExpr> {
        self.execute
            .parameters
            .clone()
            .into_iter()
            .map(|t| t.into())
            .collect()
    }
}

#[pyclass(name = "Deallocate", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyDeallocate {
    deallocate: Deallocate,
}

impl From<Deallocate> for PyDeallocate {
    fn from(deallocate: Deallocate) -> PyDeallocate {
        PyDeallocate { deallocate }
    }
}

impl TryFrom<PyDeallocate> for Deallocate {
    type Error = PyErr;

    fn try_from(py: PyDeallocate) -> Result<Self, Self::Error> {
        Ok(py.deallocate)
    }
}

impl LogicalNode for PyDeallocate {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}

#[pymethods]
impl PyDeallocate {
    #[new]
    pub fn new(name: String) -> Self {
        PyDeallocate {
            deallocate: Deallocate { name },
        }
    }

    pub fn name(&self) -> String {
        self.deallocate.name.clone()
    }
}
