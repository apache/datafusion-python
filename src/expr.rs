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

use datafusion::logical_expr::expr::AggregateFunctionParams;
use datafusion::logical_expr::utils::exprlist_to_fields;
use datafusion::logical_expr::{
    lit_with_metadata, ExprFuncBuilder, ExprFunctionExt, LogicalPlan, WindowFunctionDefinition,
};
use pyo3::IntoPyObjectExt;
use pyo3::{basic::CompareOp, prelude::*};
use std::collections::HashMap;
use std::convert::{From, Into};
use std::sync::Arc;
use window::PyWindowFrame;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::logical_expr::{
    col,
    expr::{AggregateFunction, InList, InSubquery, ScalarFunction, WindowFunction},
    lit, Between, BinaryExpr, Case, Cast, Expr, Like, Operator, TryCast,
};

use crate::common::data_type::{DataTypeMap, NullTreatment, PyScalarValue, RexType};
use crate::errors::{py_runtime_err, py_type_err, py_unsupported_variant_err, PyDataFusionResult};
use crate::expr::aggregate_expr::PyAggregateFunction;
use crate::expr::binary_expr::PyBinaryExpr;
use crate::expr::column::PyColumn;
use crate::expr::literal::PyLiteral;
use crate::functions::add_builder_fns_to_window;
use crate::pyarrow_util::scalar_to_pyarrow;
use crate::sql::logical::PyLogicalPlan;

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
pub mod conditional_expr;
pub mod copy_to;
pub mod create_catalog;
pub mod create_catalog_schema;
pub mod create_external_table;
pub mod create_function;
pub mod create_index;
pub mod create_memory_table;
pub mod create_view;
pub mod describe_table;
pub mod distinct;
pub mod dml;
pub mod drop_catalog_schema;
pub mod drop_function;
pub mod drop_table;
pub mod drop_view;
pub mod empty_relation;
pub mod exists;
pub mod explain;
pub mod extension;
pub mod filter;
pub mod grouping_set;
pub mod in_list;
pub mod in_subquery;
pub mod join;
pub mod like;
pub mod limit;
pub mod literal;
pub mod logical_node;
pub mod placeholder;
pub mod projection;
pub mod recursive_query;
pub mod repartition;
pub mod scalar_subquery;
pub mod scalar_variable;
pub mod signature;
pub mod sort;
pub mod sort_expr;
pub mod statement;
pub mod subquery;
pub mod subquery_alias;
pub mod table_scan;
pub mod union;
pub mod unnest;
pub mod unnest_expr;
pub mod values;
pub mod window;

use sort_expr::{to_sort_expressions, PySortExpr};

/// A PyExpr that can be used on a DataFrame
#[pyclass(name = "RawExpr", module = "datafusion.expr", subclass)]
#[derive(Debug, Clone)]
pub struct PyExpr {
    pub expr: Expr,
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

/// Convert a list of DataFusion Expr to PyExpr
pub fn py_expr_list(expr: &[Expr]) -> PyResult<Vec<PyExpr>> {
    Ok(expr.iter().map(|e| PyExpr::from(e.clone())).collect())
}

#[pymethods]
impl PyExpr {
    /// Return the specific expression
    fn to_variant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Python::with_gil(|_| {
            match &self.expr {
            Expr::Alias(alias) => Ok(PyAlias::from(alias.clone()).into_bound_py_any(py)?),
            Expr::Column(col) => Ok(PyColumn::from(col.clone()).into_bound_py_any(py)?),
            Expr::ScalarVariable(data_type, variables) => {
                Ok(PyScalarVariable::new(data_type, variables).into_bound_py_any(py)?)
            }
            Expr::Like(value) => Ok(PyLike::from(value.clone()).into_bound_py_any(py)?),
            Expr::Literal(value, metadata) => Ok(PyLiteral::new_with_metadata(value.clone(), metadata.clone()).into_bound_py_any(py)?),
            Expr::BinaryExpr(expr) => Ok(PyBinaryExpr::from(expr.clone()).into_bound_py_any(py)?),
            Expr::Not(expr) => Ok(PyNot::new(*expr.clone()).into_bound_py_any(py)?),
            Expr::IsNotNull(expr) => Ok(PyIsNotNull::new(*expr.clone()).into_bound_py_any(py)?),
            Expr::IsNull(expr) => Ok(PyIsNull::new(*expr.clone()).into_bound_py_any(py)?),
            Expr::IsTrue(expr) => Ok(PyIsTrue::new(*expr.clone()).into_bound_py_any(py)?),
            Expr::IsFalse(expr) => Ok(PyIsFalse::new(*expr.clone()).into_bound_py_any(py)?),
            Expr::IsUnknown(expr) => Ok(PyIsUnknown::new(*expr.clone()).into_bound_py_any(py)?),
            Expr::IsNotTrue(expr) => Ok(PyIsNotTrue::new(*expr.clone()).into_bound_py_any(py)?),
            Expr::IsNotFalse(expr) => Ok(PyIsNotFalse::new(*expr.clone()).into_bound_py_any(py)?),
            Expr::IsNotUnknown(expr) => Ok(PyIsNotUnknown::new(*expr.clone()).into_bound_py_any(py)?),
            Expr::Negative(expr) => Ok(PyNegative::new(*expr.clone()).into_bound_py_any(py)?),
            Expr::AggregateFunction(expr) => {
                Ok(PyAggregateFunction::from(expr.clone()).into_bound_py_any(py)?)
            }
            Expr::SimilarTo(value) => Ok(PySimilarTo::from(value.clone()).into_bound_py_any(py)?),
            Expr::Between(value) => Ok(between::PyBetween::from(value.clone()).into_bound_py_any(py)?),
            Expr::Case(value) => Ok(case::PyCase::from(value.clone()).into_bound_py_any(py)?),
            Expr::Cast(value) => Ok(cast::PyCast::from(value.clone()).into_bound_py_any(py)?),
            Expr::TryCast(value) => Ok(cast::PyTryCast::from(value.clone()).into_bound_py_any(py)?),
            Expr::ScalarFunction(value) => Err(py_unsupported_variant_err(format!(
                "Converting Expr::ScalarFunction to a Python object is not implemented: {value:?}"
            ))),
            Expr::WindowFunction(value) => Err(py_unsupported_variant_err(format!(
                "Converting Expr::WindowFunction to a Python object is not implemented: {value:?}"
            ))),
            Expr::InList(value) => Ok(in_list::PyInList::from(value.clone()).into_bound_py_any(py)?),
            Expr::Exists(value) => Ok(exists::PyExists::from(value.clone()).into_bound_py_any(py)?),
            Expr::InSubquery(value) => {
                Ok(in_subquery::PyInSubquery::from(value.clone()).into_bound_py_any(py)?)
            }
            Expr::ScalarSubquery(value) => {
                Ok(scalar_subquery::PyScalarSubquery::from(value.clone()).into_bound_py_any(py)?)
            }
            #[allow(deprecated)]
            Expr::Wildcard { qualifier, options } => Err(py_unsupported_variant_err(format!(
                "Converting Expr::Wildcard to a Python object is not implemented : {qualifier:?} {options:?}"
            ))),
            Expr::GroupingSet(value) => {
                Ok(grouping_set::PyGroupingSet::from(value.clone()).into_bound_py_any(py)?)
            }
            Expr::Placeholder(value) => {
                Ok(placeholder::PyPlaceholder::from(value.clone()).into_bound_py_any(py)?)
            }
            Expr::OuterReferenceColumn(data_type, column) => Err(py_unsupported_variant_err(format!(
                "Converting Expr::OuterReferenceColumn to a Python object is not implemented: {data_type:?} - {column:?}"
            ))),
            Expr::Unnest(value) => Ok(unnest_expr::PyUnnestExpr::from(value.clone()).into_bound_py_any(py)?),
        }
        })
    }

    /// Returns the name of this expression as it should appear in a schema. This name
    /// will not include any CAST expressions.
    fn schema_name(&self) -> PyResult<String> {
        Ok(format!("{}", self.expr.schema_name()))
    }

    /// Returns a full and complete string representation of this expression.
    fn canonical_name(&self) -> PyResult<String> {
        Ok(format!("{}", self.expr))
    }

    /// Returns the name of the Expr variant.
    /// Ex: 'IsNotNull', 'Literal', 'BinaryExpr', etc
    fn variant_name(&self) -> PyResult<&str> {
        Ok(self.expr.variant_name())
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
        let expr = self.expr.clone() % rhs.expr;
        Ok(expr.into())
    }

    fn __and__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(self.expr.clone().and(rhs.expr).into())
    }

    fn __or__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(self.expr.clone().or(rhs.expr).into())
    }

    fn __invert__(&self) -> PyResult<PyExpr> {
        let expr = !self.expr.clone();
        Ok(expr.into())
    }

    fn __getitem__(&self, key: &str) -> PyResult<PyExpr> {
        Ok(self.expr.clone().field(key).into())
    }

    #[staticmethod]
    pub fn literal(value: PyScalarValue) -> PyExpr {
        lit(value.0).into()
    }

    #[staticmethod]
    pub fn literal_with_metadata(
        value: PyScalarValue,
        metadata: HashMap<String, String>,
    ) -> PyExpr {
        lit_with_metadata(value.0, metadata).into()
    }

    #[staticmethod]
    pub fn column(value: &str) -> PyExpr {
        col(value).into()
    }

    /// assign a name to the PyExpr
    #[pyo3(signature = (name, metadata=None))]
    pub fn alias(&self, name: &str, metadata: Option<HashMap<String, String>>) -> PyExpr {
        self.expr.clone().alias_with_metadata(name, metadata).into()
    }

    /// Create a sort PyExpr from an existing PyExpr.
    #[pyo3(signature = (ascending=true, nulls_first=true))]
    pub fn sort(&self, ascending: bool, nulls_first: bool) -> PySortExpr {
        self.expr.clone().sort(ascending, nulls_first).into()
    }

    pub fn is_null(&self) -> PyExpr {
        self.expr.clone().is_null().into()
    }

    pub fn is_not_null(&self) -> PyExpr {
        self.expr.clone().is_not_null().into()
    }

    pub fn cast(&self, to: PyArrowType<DataType>) -> PyExpr {
        // self.expr.cast_to() requires DFSchema to validate that the cast
        // is supported, omit that for now
        let expr = Expr::Cast(Cast::new(Box::new(self.expr.clone()), to.0));
        expr.into()
    }

    #[pyo3(signature = (low, high, negated=false))]
    pub fn between(&self, low: PyExpr, high: PyExpr, negated: bool) -> PyExpr {
        let expr = Expr::Between(Between::new(
            Box::new(self.expr.clone()),
            negated,
            Box::new(low.into()),
            Box::new(high.into()),
        ));
        expr.into()
    }

    /// A Rex (Row Expression) specifies a single row of data. That specification
    /// could include user defined functions or types. RexType identifies the row
    /// as one of the possible valid `RexTypes`.
    pub fn rex_type(&self) -> PyResult<RexType> {
        Ok(match self.expr {
            Expr::Alias(..) => RexType::Alias,
            Expr::Column(..) => RexType::Reference,
            Expr::ScalarVariable(..) | Expr::Literal(..) => RexType::Literal,
            Expr::BinaryExpr { .. }
            | Expr::Not(..)
            | Expr::IsNotNull(..)
            | Expr::Negative(..)
            | Expr::IsNull(..)
            | Expr::Like { .. }
            | Expr::SimilarTo { .. }
            | Expr::Between { .. }
            | Expr::Case { .. }
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::ScalarFunction { .. }
            | Expr::AggregateFunction { .. }
            | Expr::WindowFunction { .. }
            | Expr::InList { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::GroupingSet(..)
            | Expr::IsTrue(..)
            | Expr::IsFalse(..)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(..)
            | Expr::IsNotFalse(..)
            | Expr::Placeholder { .. }
            | Expr::OuterReferenceColumn(_, _)
            | Expr::Unnest(_)
            | Expr::IsNotUnknown(_) => RexType::Call,
            Expr::ScalarSubquery(..) => RexType::ScalarSubquery,
            #[allow(deprecated)]
            Expr::Wildcard { .. } => {
                return Err(py_unsupported_variant_err("Expr::Wildcard is unsupported"))
            }
        })
    }

    /// Given the current `Expr` return the DataTypeMap which represents the
    /// PythonType, Arrow DataType, and SqlType Enum which represents
    pub fn types(&self) -> PyResult<DataTypeMap> {
        Self::_types(&self.expr)
    }

    /// Extracts the Expr value into a PyObject that can be shared with Python
    pub fn python_value(&self, py: Python) -> PyResult<PyObject> {
        match &self.expr {
            Expr::Literal(scalar_value, _) => scalar_to_pyarrow(scalar_value, py),
            _ => Err(py_type_err(format!(
                "Non Expr::Literal encountered in types: {:?}",
                &self.expr
            ))),
        }
    }

    /// Row expressions, Rex(s), operate on the concept of operands. Different variants of Expressions, Expr(s),
    /// store those operands in different datastructures. This function examines the Expr variant and returns
    /// the operands to the calling logic as a Vec of PyExpr instances.
    pub fn rex_call_operands(&self) -> PyResult<Vec<PyExpr>> {
        match &self.expr {
            // Expr variants that are themselves the operand to return
            Expr::Column(..) | Expr::ScalarVariable(..) | Expr::Literal(..) => {
                Ok(vec![PyExpr::from(self.expr.clone())])
            }

            Expr::Alias(alias) => Ok(vec![PyExpr::from(*alias.expr.clone())]),

            // Expr(s) that house the Expr instance to return in their bounded params
            Expr::Not(expr)
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::Negative(expr)
            | Expr::Cast(Cast { expr, .. })
            | Expr::TryCast(TryCast { expr, .. })
            | Expr::InSubquery(InSubquery { expr, .. }) => Ok(vec![PyExpr::from(*expr.clone())]),

            // Expr variants containing a collection of Expr(s) for operands
            Expr::AggregateFunction(AggregateFunction {
                params: AggregateFunctionParams { args, .. },
                ..
            })
            | Expr::ScalarFunction(ScalarFunction { args, .. }) => {
                Ok(args.iter().map(|arg| PyExpr::from(arg.clone())).collect())
            }
            Expr::WindowFunction(boxed_window_fn) => {
                let args = &boxed_window_fn.params.args;
                Ok(args.iter().map(|arg| PyExpr::from(arg.clone())).collect())
            }

            // Expr(s) that require more specific processing
            Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }) => {
                let mut operands: Vec<PyExpr> = Vec::new();

                if let Some(e) = expr {
                    for (when, then) in when_then_expr {
                        operands.push(PyExpr::from(Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(*e.clone()),
                            Operator::Eq,
                            Box::new(*when.clone()),
                        ))));
                        operands.push(PyExpr::from(*then.clone()));
                    }
                } else {
                    for (when, then) in when_then_expr {
                        operands.push(PyExpr::from(*when.clone()));
                        operands.push(PyExpr::from(*then.clone()));
                    }
                };

                if let Some(e) = else_expr {
                    operands.push(PyExpr::from(*e.clone()));
                };

                Ok(operands)
            }
            Expr::InList(InList { expr, list, .. }) => {
                let mut operands: Vec<PyExpr> = vec![PyExpr::from(*expr.clone())];
                for list_elem in list {
                    operands.push(PyExpr::from(list_elem.clone()));
                }

                Ok(operands)
            }
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => Ok(vec![
                PyExpr::from(*left.clone()),
                PyExpr::from(*right.clone()),
            ]),
            Expr::Like(Like { expr, pattern, .. }) => Ok(vec![
                PyExpr::from(*expr.clone()),
                PyExpr::from(*pattern.clone()),
            ]),
            Expr::SimilarTo(Like { expr, pattern, .. }) => Ok(vec![
                PyExpr::from(*expr.clone()),
                PyExpr::from(*pattern.clone()),
            ]),
            Expr::Between(Between {
                expr,
                negated: _,
                low,
                high,
            }) => Ok(vec![
                PyExpr::from(*expr.clone()),
                PyExpr::from(*low.clone()),
                PyExpr::from(*high.clone()),
            ]),

            // Currently un-support/implemented Expr types for Rex Call operations
            Expr::GroupingSet(..)
            | Expr::Unnest(_)
            | Expr::OuterReferenceColumn(_, _)
            | Expr::ScalarSubquery(..)
            | Expr::Placeholder { .. }
            | Expr::Exists { .. } => Err(py_runtime_err(format!(
                "Unimplemented Expr type: {}",
                self.expr
            ))),

            #[allow(deprecated)]
            Expr::Wildcard { .. } => {
                Err(py_unsupported_variant_err("Expr::Wildcard is unsupported"))
            }
        }
    }

    /// Extracts the operator associated with a RexType::Call
    pub fn rex_call_operator(&self) -> PyResult<String> {
        Ok(match &self.expr {
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op,
                right: _,
            }) => format!("{op}"),
            Expr::ScalarFunction(ScalarFunction { func, args: _ }) => func.name().to_string(),
            Expr::Cast { .. } => "cast".to_string(),
            Expr::Between { .. } => "between".to_string(),
            Expr::Case { .. } => "case".to_string(),
            Expr::IsNull(..) => "is null".to_string(),
            Expr::IsNotNull(..) => "is not null".to_string(),
            Expr::IsTrue(_) => "is true".to_string(),
            Expr::IsFalse(_) => "is false".to_string(),
            Expr::IsUnknown(_) => "is unknown".to_string(),
            Expr::IsNotTrue(_) => "is not true".to_string(),
            Expr::IsNotFalse(_) => "is not false".to_string(),
            Expr::IsNotUnknown(_) => "is not unknown".to_string(),
            Expr::InList { .. } => "in list".to_string(),
            Expr::Negative(..) => "negative".to_string(),
            Expr::Not(..) => "not".to_string(),
            Expr::Like(Like {
                negated,
                case_insensitive,
                ..
            }) => {
                let name = if *case_insensitive { "ilike" } else { "like" };
                if *negated {
                    format!("not {name}")
                } else {
                    name.to_string()
                }
            }
            Expr::SimilarTo(Like { negated, .. }) => {
                if *negated {
                    "not similar to".to_string()
                } else {
                    "similar to".to_string()
                }
            }
            _ => {
                return Err(py_type_err(format!(
                    "Catch all triggered in get_operator_name: {:?}",
                    &self.expr
                )))
            }
        })
    }

    pub fn column_name(&self, plan: PyLogicalPlan) -> PyResult<String> {
        self._column_name(&plan.plan()).map_err(py_runtime_err)
    }

    // Expression Function Builder functions

    pub fn order_by(&self, order_by: Vec<PySortExpr>) -> PyExprFuncBuilder {
        self.expr
            .clone()
            .order_by(to_sort_expressions(order_by))
            .into()
    }

    pub fn filter(&self, filter: PyExpr) -> PyExprFuncBuilder {
        self.expr.clone().filter(filter.expr.clone()).into()
    }

    pub fn distinct(&self) -> PyExprFuncBuilder {
        self.expr.clone().distinct().into()
    }

    pub fn null_treatment(&self, null_treatment: NullTreatment) -> PyExprFuncBuilder {
        self.expr
            .clone()
            .null_treatment(Some(null_treatment.into()))
            .into()
    }

    pub fn partition_by(&self, partition_by: Vec<PyExpr>) -> PyExprFuncBuilder {
        let partition_by = partition_by.iter().map(|e| e.expr.clone()).collect();
        self.expr.clone().partition_by(partition_by).into()
    }

    pub fn window_frame(&self, window_frame: PyWindowFrame) -> PyExprFuncBuilder {
        self.expr.clone().window_frame(window_frame.into()).into()
    }

    #[pyo3(signature = (partition_by=None, window_frame=None, order_by=None, null_treatment=None))]
    pub fn over(
        &self,
        partition_by: Option<Vec<PyExpr>>,
        window_frame: Option<PyWindowFrame>,
        order_by: Option<Vec<PySortExpr>>,
        null_treatment: Option<NullTreatment>,
    ) -> PyDataFusionResult<PyExpr> {
        match &self.expr {
            Expr::AggregateFunction(agg_fn) => {
                let window_fn = Expr::WindowFunction(Box::new(WindowFunction::new(
                    WindowFunctionDefinition::AggregateUDF(agg_fn.func.clone()),
                    agg_fn.params.args.clone(),
                )));

                add_builder_fns_to_window(
                    window_fn,
                    partition_by,
                    window_frame,
                    order_by,
                    null_treatment,
                )
            }
            Expr::WindowFunction(_) => add_builder_fns_to_window(
                self.expr.clone(),
                partition_by,
                window_frame,
                order_by,
                null_treatment,
            ),
            _ => Err(datafusion::error::DataFusionError::Plan(format!(
                "Using {} with `over` is not allowed. Must use an aggregate or window function.",
                self.expr.variant_name()
            ))
            .into()),
        }
    }
}

#[pyclass(name = "ExprFuncBuilder", module = "datafusion.expr", subclass)]
#[derive(Debug, Clone)]
pub struct PyExprFuncBuilder {
    pub builder: ExprFuncBuilder,
}

impl From<ExprFuncBuilder> for PyExprFuncBuilder {
    fn from(builder: ExprFuncBuilder) -> Self {
        Self { builder }
    }
}

#[pymethods]
impl PyExprFuncBuilder {
    pub fn order_by(&self, order_by: Vec<PySortExpr>) -> PyExprFuncBuilder {
        self.builder
            .clone()
            .order_by(to_sort_expressions(order_by))
            .into()
    }

    pub fn filter(&self, filter: PyExpr) -> PyExprFuncBuilder {
        self.builder.clone().filter(filter.expr.clone()).into()
    }

    pub fn distinct(&self) -> PyExprFuncBuilder {
        self.builder.clone().distinct().into()
    }

    pub fn null_treatment(&self, null_treatment: NullTreatment) -> PyExprFuncBuilder {
        self.builder
            .clone()
            .null_treatment(Some(null_treatment.into()))
            .into()
    }

    pub fn partition_by(&self, partition_by: Vec<PyExpr>) -> PyExprFuncBuilder {
        let partition_by = partition_by.iter().map(|e| e.expr.clone()).collect();
        self.builder.clone().partition_by(partition_by).into()
    }

    pub fn window_frame(&self, window_frame: PyWindowFrame) -> PyExprFuncBuilder {
        self.builder
            .clone()
            .window_frame(window_frame.into())
            .into()
    }

    pub fn build(&self) -> PyDataFusionResult<PyExpr> {
        Ok(self.builder.clone().build().map(|expr| expr.into())?)
    }
}

impl PyExpr {
    pub fn _column_name(&self, plan: &LogicalPlan) -> PyDataFusionResult<String> {
        let field = Self::expr_to_field(&self.expr, plan)?;
        Ok(field.name().to_owned())
    }

    /// Create a [Field] representing an [Expr], given an input [LogicalPlan] to resolve against
    pub fn expr_to_field(expr: &Expr, input_plan: &LogicalPlan) -> PyDataFusionResult<Arc<Field>> {
        let fields = exprlist_to_fields(&[expr.clone()], input_plan)?;
        Ok(fields[0].1.clone())
    }
    fn _types(expr: &Expr) -> PyResult<DataTypeMap> {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op,
                right: _,
            }) => match op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::And
                | Operator::Or
                | Operator::IsDistinctFrom
                | Operator::IsNotDistinctFrom
                | Operator::RegexMatch
                | Operator::RegexIMatch
                | Operator::RegexNotMatch
                | Operator::RegexNotIMatch
                | Operator::LikeMatch
                | Operator::ILikeMatch
                | Operator::NotLikeMatch
                | Operator::NotILikeMatch => DataTypeMap::map_from_arrow_type(&DataType::Boolean),
                Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Modulo => {
                    DataTypeMap::map_from_arrow_type(&DataType::Int64)
                }
                Operator::Divide => DataTypeMap::map_from_arrow_type(&DataType::Float64),
                Operator::StringConcat => DataTypeMap::map_from_arrow_type(&DataType::Utf8),
                Operator::BitwiseShiftLeft
                | Operator::BitwiseShiftRight
                | Operator::BitwiseXor
                | Operator::BitwiseAnd
                | Operator::BitwiseOr => DataTypeMap::map_from_arrow_type(&DataType::Binary),
                Operator::AtArrow
                | Operator::ArrowAt
                | Operator::Arrow
                | Operator::LongArrow
                | Operator::HashArrow
                | Operator::HashLongArrow
                | Operator::AtAt
                | Operator::IntegerDivide
                | Operator::HashMinus
                | Operator::AtQuestion
                | Operator::Question
                | Operator::QuestionAnd
                | Operator::QuestionPipe => Err(py_type_err(format!("Unsupported expr: ${op}"))),
            },
            Expr::Cast(Cast { expr: _, data_type }) => DataTypeMap::map_from_arrow_type(data_type),
            Expr::Literal(scalar_value, _) => DataTypeMap::map_from_scalar_value(scalar_value),
            _ => Err(py_type_err(format!(
                "Non Expr::Literal encountered in types: {expr:?}"
            ))),
        }
    }
}

/// Initializes the `expr` module to match the pattern of `datafusion-expr` https://docs.rs/datafusion-expr/latest/datafusion_expr/
pub(crate) fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
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
    m.add_class::<in_list::PyInList>()?;
    m.add_class::<exists::PyExists>()?;
    m.add_class::<subquery::PySubquery>()?;
    m.add_class::<in_subquery::PyInSubquery>()?;
    m.add_class::<scalar_subquery::PyScalarSubquery>()?;
    m.add_class::<placeholder::PyPlaceholder>()?;
    m.add_class::<grouping_set::PyGroupingSet>()?;
    m.add_class::<case::PyCase>()?;
    m.add_class::<conditional_expr::PyCaseBuilder>()?;
    m.add_class::<cast::PyCast>()?;
    m.add_class::<cast::PyTryCast>()?;
    m.add_class::<between::PyBetween>()?;
    m.add_class::<explain::PyExplain>()?;
    m.add_class::<limit::PyLimit>()?;
    m.add_class::<aggregate::PyAggregate>()?;
    m.add_class::<sort::PySort>()?;
    m.add_class::<analyze::PyAnalyze>()?;
    m.add_class::<empty_relation::PyEmptyRelation>()?;
    m.add_class::<join::PyJoin>()?;
    m.add_class::<join::PyJoinType>()?;
    m.add_class::<join::PyJoinConstraint>()?;
    m.add_class::<union::PyUnion>()?;
    m.add_class::<unnest::PyUnnest>()?;
    m.add_class::<unnest_expr::PyUnnestExpr>()?;
    m.add_class::<extension::PyExtension>()?;
    m.add_class::<filter::PyFilter>()?;
    m.add_class::<projection::PyProjection>()?;
    m.add_class::<table_scan::PyTableScan>()?;
    m.add_class::<create_memory_table::PyCreateMemoryTable>()?;
    m.add_class::<create_view::PyCreateView>()?;
    m.add_class::<distinct::PyDistinct>()?;
    m.add_class::<sort_expr::PySortExpr>()?;
    m.add_class::<subquery_alias::PySubqueryAlias>()?;
    m.add_class::<drop_table::PyDropTable>()?;
    m.add_class::<repartition::PyPartitioning>()?;
    m.add_class::<repartition::PyRepartition>()?;
    m.add_class::<window::PyWindowExpr>()?;
    m.add_class::<window::PyWindowFrame>()?;
    m.add_class::<window::PyWindowFrameBound>()?;
    m.add_class::<copy_to::PyCopyTo>()?;
    m.add_class::<copy_to::PyFileType>()?;
    m.add_class::<create_catalog::PyCreateCatalog>()?;
    m.add_class::<create_catalog_schema::PyCreateCatalogSchema>()?;
    m.add_class::<create_external_table::PyCreateExternalTable>()?;
    m.add_class::<create_function::PyCreateFunction>()?;
    m.add_class::<create_function::PyOperateFunctionArg>()?;
    m.add_class::<create_function::PyCreateFunctionBody>()?;
    m.add_class::<create_index::PyCreateIndex>()?;
    m.add_class::<describe_table::PyDescribeTable>()?;
    m.add_class::<dml::PyDmlStatement>()?;
    m.add_class::<drop_catalog_schema::PyDropCatalogSchema>()?;
    m.add_class::<drop_function::PyDropFunction>()?;
    m.add_class::<drop_view::PyDropView>()?;
    m.add_class::<recursive_query::PyRecursiveQuery>()?;

    m.add_class::<statement::PyTransactionStart>()?;
    m.add_class::<statement::PyTransactionEnd>()?;
    m.add_class::<statement::PySetVariable>()?;
    m.add_class::<statement::PyPrepare>()?;
    m.add_class::<statement::PyExecute>()?;
    m.add_class::<statement::PyDeallocate>()?;
    m.add_class::<values::PyValues>()?;
    m.add_class::<statement::PyTransactionAccessMode>()?;
    m.add_class::<statement::PyTransactionConclusion>()?;
    m.add_class::<statement::PyTransactionIsolationLevel>()?;

    Ok(())
}
