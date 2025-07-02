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

use datafusion::logical_expr::expr::Like;
use pyo3::prelude::*;
use std::fmt::{self, Display, Formatter};

use crate::expr::PyExpr;

#[pyclass(name = "Like", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyLike {
    like: Like,
}

impl From<Like> for PyLike {
    fn from(like: Like) -> PyLike {
        PyLike { like }
    }
}

impl From<PyLike> for Like {
    fn from(like: PyLike) -> Self {
        like.like
    }
}

impl Display for PyLike {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Like
            Negated: {:?}
            Expr: {:?}
            Pattern: {:?}
            Escape_Char: {:?}",
            &self.negated(),
            &self.expr(),
            &self.pattern(),
            &self.escape_char()
        )
    }
}

#[pymethods]
impl PyLike {
    fn negated(&self) -> PyResult<bool> {
        Ok(self.like.negated)
    }

    fn expr(&self) -> PyResult<PyExpr> {
        Ok((*self.like.expr).clone().into())
    }

    fn pattern(&self) -> PyResult<PyExpr> {
        Ok((*self.like.pattern).clone().into())
    }

    fn escape_char(&self) -> PyResult<Option<char>> {
        Ok(self.like.escape_char)
    }

    fn __repr__(&self) -> String {
        format!("Like({self})")
    }
}

#[pyclass(name = "ILike", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyILike {
    like: Like,
}

impl From<Like> for PyILike {
    fn from(like: Like) -> PyILike {
        PyILike { like }
    }
}

impl From<PyILike> for Like {
    fn from(like: PyILike) -> Self {
        like.like
    }
}

impl Display for PyILike {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "ILike
            Negated: {:?}
            Expr: {:?}
            Pattern: {:?}
            Escape_Char: {:?}",
            &self.negated(),
            &self.expr(),
            &self.pattern(),
            &self.escape_char()
        )
    }
}

#[pymethods]
impl PyILike {
    fn negated(&self) -> PyResult<bool> {
        Ok(self.like.negated)
    }

    fn expr(&self) -> PyResult<PyExpr> {
        Ok((*self.like.expr).clone().into())
    }

    fn pattern(&self) -> PyResult<PyExpr> {
        Ok((*self.like.pattern).clone().into())
    }

    fn escape_char(&self) -> PyResult<Option<char>> {
        Ok(self.like.escape_char)
    }

    fn __repr__(&self) -> String {
        format!("Like({self})")
    }
}

#[pyclass(name = "SimilarTo", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PySimilarTo {
    like: Like,
}

impl From<Like> for PySimilarTo {
    fn from(like: Like) -> PySimilarTo {
        PySimilarTo { like }
    }
}

impl From<PySimilarTo> for Like {
    fn from(like: PySimilarTo) -> Self {
        like.like
    }
}

impl Display for PySimilarTo {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "SimilarTo
            Negated: {:?}
            Expr: {:?}
            Pattern: {:?}
            Escape_Char: {:?}",
            &self.negated(),
            &self.expr(),
            &self.pattern(),
            &self.escape_char()
        )
    }
}

#[pymethods]
impl PySimilarTo {
    fn negated(&self) -> PyResult<bool> {
        Ok(self.like.negated)
    }

    fn expr(&self) -> PyResult<PyExpr> {
        Ok((*self.like.expr).clone().into())
    }

    fn pattern(&self) -> PyResult<PyExpr> {
        Ok((*self.like.pattern).clone().into())
    }

    fn escape_char(&self) -> PyResult<Option<char>> {
        Ok(self.like.escape_char)
    }

    fn __repr__(&self) -> String {
        format!("Like({self})")
    }
}
