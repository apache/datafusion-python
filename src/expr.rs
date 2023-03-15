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

use pyo3::{basic::CompareOp, prelude::*};
use std::convert::{From, Into};

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion_expr::{col, lit, Cast, Expr, GetIndexedField};

use crate::errors::py_runtime_err;
use crate::expr::aggregate_expr::PyAggregateFunction;
use crate::expr::binary_expr::PyBinaryExpr;
use crate::expr::column::PyColumn;
use crate::expr::literal::PyLiteral;
use datafusion::scalar::ScalarValue;

use self::alias::PyAlias;
use self::bool_expr::{
    PyIsFalse, PyIsNotFalse, PyIsNotNull, PyIsNotTrue, PyIsNotUnknown, PyIsNull, PyIsTrue,
    PyIsUnknown, PyNegative, PyNot,
};
use self::like::{PyILike, PyLike, PySimilarTo};
use self::scalar_variable::PyScalarVariable;

pub mod aggregate;
pub mod aggregate_expr;
pub mod alias;
pub mod analyze;
pub mod between;
pub mod binary_expr;
pub mod bool_expr;
pub mod case;
pub mod cast;
pub mod column;
pub mod create_memory_table;
pub mod create_view;
pub mod cross_join;
pub mod distinct;
pub mod drop_table;
pub mod empty_relation;
pub mod exists;
pub mod explain;
pub mod extension;
pub mod filter;
pub mod grouping_set;
pub mod in_list;
pub mod in_subquery;
pub mod indexed_field;
pub mod join;
pub mod like;
pub mod limit;
pub mod literal;
pub mod logical_node;
pub mod placeholder;
pub mod projection;
pub mod repartition;
pub mod scalar_function;
pub mod scalar_subquery;
pub mod scalar_variable;
pub mod signature;
pub mod sort;
pub mod subquery;
pub mod subquery_alias;
pub mod table_scan;
pub mod union;

/// A PyExpr that can be used on a DataFrame
#[pyclass(name = "Expr", module = "datafusion.expr", subclass)]
#[derive(Debug, Clone)]
pub struct PyExpr {
    pub(crate) expr: Expr,
}

impl From<PyExpr> for Expr {
    fn from(expr: PyExpr) -> Expr {
        expr.expr
    }
}

impl From<Expr> for PyExpr {
    fn from(expr: Expr) -> PyExpr {
        PyExpr { expr }
    }
}

#[pymethods]
impl PyExpr {
    /// Return the specific expression
    fn to_variant(&self, py: Python) -> PyResult<PyObject> {
        Python::with_gil(|_| match &self.expr {
            Expr::Alias(alias, name) => Ok(PyAlias::new(alias, name).into_py(py)),
            Expr::Column(col) => Ok(PyColumn::from(col.clone()).into_py(py)),
            Expr::ScalarVariable(data_type, variables) => {
                Ok(PyScalarVariable::new(data_type, variables).into_py(py))
            }
            Expr::Literal(value) => Ok(PyLiteral::from(value.clone()).into_py(py)),
            Expr::BinaryExpr(expr) => Ok(PyBinaryExpr::from(expr.clone()).into_py(py)),
            Expr::Not(expr) => Ok(PyNot::new(*expr.clone()).into_py(py)),
            Expr::IsNotNull(expr) => Ok(PyIsNotNull::new(*expr.clone()).into_py(py)),
            Expr::IsNull(expr) => Ok(PyIsNull::new(*expr.clone()).into_py(py)),
            Expr::IsTrue(expr) => Ok(PyIsTrue::new(*expr.clone()).into_py(py)),
            Expr::IsFalse(expr) => Ok(PyIsFalse::new(*expr.clone()).into_py(py)),
            Expr::IsUnknown(expr) => Ok(PyIsUnknown::new(*expr.clone()).into_py(py)),
            Expr::IsNotTrue(expr) => Ok(PyIsNotTrue::new(*expr.clone()).into_py(py)),
            Expr::IsNotFalse(expr) => Ok(PyIsNotFalse::new(*expr.clone()).into_py(py)),
            Expr::IsNotUnknown(expr) => Ok(PyIsNotUnknown::new(*expr.clone()).into_py(py)),
            Expr::Negative(expr) => Ok(PyNegative::new(*expr.clone()).into_py(py)),
            Expr::AggregateFunction(expr) => {
                Ok(PyAggregateFunction::from(expr.clone()).into_py(py))
            }
            other => Err(py_runtime_err(format!(
                "Cannot convert this Expr to a Python object: {:?}",
                other
            ))),
        })
    }

    /// Returns the name of this expression as it should appear in a schema. This name
    /// will not include any CAST expressions.
    fn display_name(&self) -> PyResult<String> {
        Ok(self.expr.display_name()?)
    }

    /// Returns a full and complete string representation of this expression.
    fn canonical_name(&self) -> PyResult<String> {
        Ok(self.expr.canonical_name())
    }

    fn __richcmp__(&self, other: PyExpr, op: CompareOp) -> PyExpr {
        let expr = match op {
            CompareOp::Lt => self.expr.clone().lt(other.expr),
            CompareOp::Le => self.expr.clone().lt_eq(other.expr),
            CompareOp::Eq => self.expr.clone().eq(other.expr),
            CompareOp::Ne => self.expr.clone().not_eq(other.expr),
            CompareOp::Gt => self.expr.clone().gt(other.expr),
            CompareOp::Ge => self.expr.clone().gt_eq(other.expr),
        };
        expr.into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Expr({})", self.expr))
    }

    fn __add__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok((self.expr.clone() + rhs.expr).into())
    }

    fn __sub__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok((self.expr.clone() - rhs.expr).into())
    }

    fn __truediv__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok((self.expr.clone() / rhs.expr).into())
    }

    fn __mul__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok((self.expr.clone() * rhs.expr).into())
    }

    fn __mod__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(self.expr.clone().modulus(rhs.expr).into())
    }

    fn __and__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(self.expr.clone().and(rhs.expr).into())
    }

    fn __or__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(self.expr.clone().or(rhs.expr).into())
    }

    fn __invert__(&self) -> PyResult<PyExpr> {
        Ok(self.expr.clone().not().into())
    }

    fn __getitem__(&self, key: &str) -> PyResult<PyExpr> {
        Ok(Expr::GetIndexedField(GetIndexedField::new(
            Box::new(self.expr.clone()),
            ScalarValue::Utf8(Some(key.to_string())),
        ))
        .into())
    }

    #[staticmethod]
    pub fn literal(value: ScalarValue) -> PyExpr {
        lit(value).into()
    }

    #[staticmethod]
    pub fn column(value: &str) -> PyExpr {
        col(value).into()
    }

    /// assign a name to the PyExpr
    pub fn alias(&self, name: &str) -> PyExpr {
        self.expr.clone().alias(name).into()
    }

    /// Create a sort PyExpr from an existing PyExpr.
    #[pyo3(signature = (ascending=true, nulls_first=true))]
    pub fn sort(&self, ascending: bool, nulls_first: bool) -> PyExpr {
        self.expr.clone().sort(ascending, nulls_first).into()
    }

    pub fn is_null(&self) -> PyExpr {
        self.expr.clone().is_null().into()
    }

    pub fn cast(&self, to: PyArrowType<DataType>) -> PyExpr {
        // self.expr.cast_to() requires DFSchema to validate that the cast
        // is supported, omit that for now
        let expr = Expr::Cast(Cast::new(Box::new(self.expr.clone()), to.0));
        expr.into()
    }
}

/// Initializes the `expr` module to match the pattern of `datafusion-expr` https://docs.rs/datafusion-expr/latest/datafusion_expr/
pub(crate) fn init_module(m: &PyModule) -> PyResult<()> {
    // expressions
    m.add_class::<PyExpr>()?;
    m.add_class::<PyColumn>()?;
    m.add_class::<PyLiteral>()?;
    m.add_class::<PyBinaryExpr>()?;
    m.add_class::<PyLiteral>()?;
    m.add_class::<PyAggregateFunction>()?;
    m.add_class::<PyNot>()?;
    m.add_class::<PyIsNotNull>()?;
    m.add_class::<PyIsNull>()?;
    m.add_class::<PyIsTrue>()?;
    m.add_class::<PyIsFalse>()?;
    m.add_class::<PyIsUnknown>()?;
    m.add_class::<PyIsNotTrue>()?;
    m.add_class::<PyIsNotFalse>()?;
    m.add_class::<PyIsNotUnknown>()?;
    m.add_class::<PyNegative>()?;
    m.add_class::<PyLike>()?;
    m.add_class::<PyILike>()?;
    m.add_class::<PySimilarTo>()?;
    m.add_class::<PyScalarVariable>()?;
    m.add_class::<alias::PyAlias>()?;
    m.add_class::<scalar_function::PyScalarFunction>()?;
    m.add_class::<scalar_function::PyBuiltinScalarFunction>()?;
    m.add_class::<in_list::PyInList>()?;
    m.add_class::<exists::PyExists>()?;
    m.add_class::<subquery::PySubquery>()?;
    m.add_class::<in_subquery::PyInSubquery>()?;
    m.add_class::<scalar_subquery::PyScalarSubquery>()?;
    m.add_class::<placeholder::PyPlaceholder>()?;
    m.add_class::<grouping_set::PyGroupingSet>()?;
    m.add_class::<case::PyCase>()?;
    m.add_class::<cast::PyCast>()?;
    m.add_class::<cast::PyTryCast>()?;
    m.add_class::<between::PyBetween>()?;
    m.add_class::<indexed_field::PyGetIndexedField>()?;
    m.add_class::<explain::PyExplain>()?;
    m.add_class::<limit::PyLimit>()?;
    m.add_class::<aggregate::PyAggregate>()?;
    m.add_class::<sort::PySort>()?;
    m.add_class::<analyze::PyAnalyze>()?;
    m.add_class::<empty_relation::PyEmptyRelation>()?;
    m.add_class::<join::PyJoin>()?;
    m.add_class::<join::PyJoinType>()?;
    m.add_class::<join::PyJoinConstraint>()?;
    m.add_class::<cross_join::PyCrossJoin>()?;
    m.add_class::<union::PyUnion>()?;
    m.add_class::<extension::PyExtension>()?;
    m.add_class::<filter::PyFilter>()?;
    m.add_class::<projection::PyProjection>()?;
    m.add_class::<table_scan::PyTableScan>()?;
    m.add_class::<create_memory_table::PyCreateMemoryTable>()?;
    m.add_class::<create_view::PyCreateView>()?;
    m.add_class::<distinct::PyDistinct>()?;
    m.add_class::<subquery_alias::PySubqueryAlias>()?;
    m.add_class::<drop_table::PyDropTable>()?;
    m.add_class::<repartition::PyPartitioning>()?;
    m.add_class::<repartition::PyRepartition>()?;
    Ok(())
}
