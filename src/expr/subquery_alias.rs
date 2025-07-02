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

use datafusion::logical_expr::SubqueryAlias;
use pyo3::{prelude::*, IntoPyObjectExt};

use crate::{common::df_schema::PyDFSchema, sql::logical::PyLogicalPlan};

use super::logical_node::LogicalNode;

#[pyclass(name = "SubqueryAlias", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PySubqueryAlias {
    subquery_alias: SubqueryAlias,
}

impl From<PySubqueryAlias> for SubqueryAlias {
    fn from(subquery_alias: PySubqueryAlias) -> Self {
        subquery_alias.subquery_alias
    }
}

impl From<SubqueryAlias> for PySubqueryAlias {
    fn from(subquery_alias: SubqueryAlias) -> PySubqueryAlias {
        PySubqueryAlias { subquery_alias }
    }
}

impl Display for PySubqueryAlias {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "SubqueryAlias
            Inputs(s): {:?}
            Alias: {:?}
            Schema: {:?}",
            self.subquery_alias.input, self.subquery_alias.alias, self.subquery_alias.schema,
        )
    }
}

#[pymethods]
impl PySubqueryAlias {
    /// Retrieves the input `LogicalPlan` to this `Projection` node
    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    /// Resulting Schema for this `Projection` node instance
    fn schema(&self) -> PyResult<PyDFSchema> {
        Ok((*self.subquery_alias.schema).clone().into())
    }

    fn alias(&self) -> PyResult<String> {
        Ok(self.subquery_alias.alias.to_string())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("SubqueryAlias({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("SubqueryAlias".to_string())
    }
}

impl LogicalNode for PySubqueryAlias {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.subquery_alias.input).clone())]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
