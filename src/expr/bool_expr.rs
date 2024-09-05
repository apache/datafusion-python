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

use datafusion::logical_expr::Expr;
use pyo3::prelude::*;
use std::fmt::{self, Display, Formatter};

use super::PyExpr;

#[pyclass(name = "Not", module = "datafusion.expr", subclass)]
#[derive(Clone, Debug)]
pub struct PyNot {
    expr: Expr,
}

impl PyNot {
    pub fn new(expr: Expr) -> Self {
        Self { expr }
    }
}

impl Display for PyNot {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Not
            Expr: {}",
            &self.expr
        )
    }
}

#[pymethods]
impl PyNot {
    fn expr(&self) -> PyResult<PyExpr> {
        Ok(self.expr.clone().into())
    }
}

#[pyclass(name = "IsNotNull", module = "datafusion.expr", subclass)]
#[derive(Clone, Debug)]
pub struct PyIsNotNull {
    expr: Expr,
}

impl PyIsNotNull {
    pub fn new(expr: Expr) -> Self {
        Self { expr }
    }
}

impl Display for PyIsNotNull {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "IsNotNull
            Expr: {}",
            &self.expr
        )
    }
}

#[pymethods]
impl PyIsNotNull {
    fn expr(&self) -> PyResult<PyExpr> {
        Ok(self.expr.clone().into())
    }
}

#[pyclass(name = "IsNull", module = "datafusion.expr", subclass)]
#[derive(Clone, Debug)]
pub struct PyIsNull {
    expr: Expr,
}

impl PyIsNull {
    pub fn new(expr: Expr) -> Self {
        Self { expr }
    }
}

impl Display for PyIsNull {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "IsNull
            Expr: {}",
            &self.expr
        )
    }
}

#[pymethods]
impl PyIsNull {
    fn expr(&self) -> PyResult<PyExpr> {
        Ok(self.expr.clone().into())
    }
}

#[pyclass(name = "IsTrue", module = "datafusion.expr", subclass)]
#[derive(Clone, Debug)]
pub struct PyIsTrue {
    expr: Expr,
}

impl PyIsTrue {
    pub fn new(expr: Expr) -> Self {
        Self { expr }
    }
}

impl Display for PyIsTrue {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "IsTrue
            Expr: {}",
            &self.expr
        )
    }
}

#[pymethods]
impl PyIsTrue {
    fn expr(&self) -> PyResult<PyExpr> {
        Ok(self.expr.clone().into())
    }
}

#[pyclass(name = "IsFalse", module = "datafusion.expr", subclass)]
#[derive(Clone, Debug)]
pub struct PyIsFalse {
    expr: Expr,
}

impl PyIsFalse {
    pub fn new(expr: Expr) -> Self {
        Self { expr }
    }
}

impl Display for PyIsFalse {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "IsFalse
            Expr: {}",
            &self.expr
        )
    }
}

#[pymethods]
impl PyIsFalse {
    fn expr(&self) -> PyResult<PyExpr> {
        Ok(self.expr.clone().into())
    }
}

#[pyclass(name = "IsUnknown", module = "datafusion.expr", subclass)]
#[derive(Clone, Debug)]
pub struct PyIsUnknown {
    expr: Expr,
}

impl PyIsUnknown {
    pub fn new(expr: Expr) -> Self {
        Self { expr }
    }
}

impl Display for PyIsUnknown {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "IsUnknown
            Expr: {}",
            &self.expr
        )
    }
}

#[pymethods]
impl PyIsUnknown {
    fn expr(&self) -> PyResult<PyExpr> {
        Ok(self.expr.clone().into())
    }
}

#[pyclass(name = "IsNotTrue", module = "datafusion.expr", subclass)]
#[derive(Clone, Debug)]
pub struct PyIsNotTrue {
    expr: Expr,
}

impl PyIsNotTrue {
    pub fn new(expr: Expr) -> Self {
        Self { expr }
    }
}

impl Display for PyIsNotTrue {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "IsNotTrue
            Expr: {}",
            &self.expr
        )
    }
}

#[pymethods]
impl PyIsNotTrue {
    fn expr(&self) -> PyResult<PyExpr> {
        Ok(self.expr.clone().into())
    }
}

#[pyclass(name = "IsNotFalse", module = "datafusion.expr", subclass)]
#[derive(Clone, Debug)]
pub struct PyIsNotFalse {
    expr: Expr,
}

impl PyIsNotFalse {
    pub fn new(expr: Expr) -> Self {
        Self { expr }
    }
}

impl Display for PyIsNotFalse {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "IsNotFalse
            Expr: {}",
            &self.expr
        )
    }
}

#[pymethods]
impl PyIsNotFalse {
    fn expr(&self) -> PyResult<PyExpr> {
        Ok(self.expr.clone().into())
    }
}

#[pyclass(name = "IsNotUnknown", module = "datafusion.expr", subclass)]
#[derive(Clone, Debug)]
pub struct PyIsNotUnknown {
    expr: Expr,
}

impl PyIsNotUnknown {
    pub fn new(expr: Expr) -> Self {
        Self { expr }
    }
}

impl Display for PyIsNotUnknown {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "IsNotUnknown
            Expr: {}",
            &self.expr
        )
    }
}

#[pymethods]
impl PyIsNotUnknown {
    fn expr(&self) -> PyResult<PyExpr> {
        Ok(self.expr.clone().into())
    }
}

#[pyclass(name = "Negative", module = "datafusion.expr", subclass)]
#[derive(Clone, Debug)]
pub struct PyNegative {
    expr: Expr,
}

impl PyNegative {
    pub fn new(expr: Expr) -> Self {
        Self { expr }
    }
}

impl Display for PyNegative {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Negative
            Expr: {}",
            &self.expr
        )
    }
}

#[pymethods]
impl PyNegative {
    fn expr(&self) -> PyResult<PyExpr> {
        Ok(self.expr.clone().into())
    }
}
