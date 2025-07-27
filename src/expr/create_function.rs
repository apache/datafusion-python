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

use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use datafusion::logical_expr::{
    CreateFunction, CreateFunctionBody, OperateFunctionArg, Volatility,
};
use pyo3::{prelude::*, IntoPyObjectExt};

use super::logical_node::LogicalNode;
use super::PyExpr;
use crate::common::{data_type::PyDataType, df_schema::PyDFSchema};
use crate::sql::logical::PyLogicalPlan;

#[pyclass(name = "CreateFunction", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyCreateFunction {
    create: CreateFunction,
}

impl From<PyCreateFunction> for CreateFunction {
    fn from(create: PyCreateFunction) -> Self {
        create.create
    }
}

impl From<CreateFunction> for PyCreateFunction {
    fn from(create: CreateFunction) -> PyCreateFunction {
        PyCreateFunction { create }
    }
}

impl Display for PyCreateFunction {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "CreateFunction: name {:?}", self.create.name)
    }
}

#[pyclass(name = "OperateFunctionArg", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyOperateFunctionArg {
    arg: OperateFunctionArg,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(eq, eq_int, name = "Volatility", module = "datafusion.expr")]
pub enum PyVolatility {
    Immutable,
    Stable,
    Volatile,
}

#[pyclass(name = "CreateFunctionBody", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyCreateFunctionBody {
    body: CreateFunctionBody,
}

#[pymethods]
impl PyCreateFunctionBody {
    pub fn language(&self) -> Option<String> {
        self.body
            .language
            .as_ref()
            .map(|language| language.to_string())
    }

    pub fn behavior(&self) -> Option<PyVolatility> {
        self.body.behavior.as_ref().map(|behavior| match behavior {
            Volatility::Immutable => PyVolatility::Immutable,
            Volatility::Stable => PyVolatility::Stable,
            Volatility::Volatile => PyVolatility::Volatile,
        })
    }

    pub fn function_body(&self) -> Option<PyExpr> {
        self.body
            .function_body
            .as_ref()
            .map(|function_body| function_body.clone().into())
    }
}

#[pymethods]
impl PyCreateFunction {
    #[new]
    #[pyo3(signature = (or_replace, temporary, name, params, schema, return_type=None, args=None))]
    pub fn new(
        or_replace: bool,
        temporary: bool,
        name: String,
        params: PyCreateFunctionBody,
        schema: PyDFSchema,
        return_type: Option<PyDataType>,
        args: Option<Vec<PyOperateFunctionArg>>,
    ) -> Self {
        PyCreateFunction {
            create: CreateFunction {
                or_replace,
                temporary,
                name,
                args: args.map(|args| args.into_iter().map(|arg| arg.arg).collect()),
                return_type: return_type.map(|return_type| return_type.data_type),
                params: params.body,
                schema: Arc::new(schema.into()),
            },
        }
    }

    pub fn or_replace(&self) -> bool {
        self.create.or_replace
    }

    pub fn temporary(&self) -> bool {
        self.create.temporary
    }

    pub fn name(&self) -> String {
        self.create.name.clone()
    }

    pub fn params(&self) -> PyCreateFunctionBody {
        PyCreateFunctionBody {
            body: self.create.params.clone(),
        }
    }

    pub fn schema(&self) -> PyDFSchema {
        (*self.create.schema).clone().into()
    }

    pub fn return_type(&self) -> Option<PyDataType> {
        self.create
            .return_type
            .as_ref()
            .map(|return_type| return_type.clone().into())
    }

    pub fn args(&self) -> Option<Vec<PyOperateFunctionArg>> {
        self.create.args.as_ref().map(|args| {
            args.iter()
                .map(|arg| PyOperateFunctionArg { arg: arg.clone() })
                .collect()
        })
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("CreateFunction({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("CreateFunction".to_string())
    }
}

impl LogicalNode for PyCreateFunction {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![]
    }

    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.clone().into_bound_py_any(py)
    }
}
