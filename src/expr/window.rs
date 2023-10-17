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

use datafusion_common::ScalarValue;
use datafusion_expr::expr::WindowFunction;
use datafusion_expr::{Expr, Window, WindowFrame, WindowFrameBound};
use pyo3::prelude::*;
use std::fmt::{self, Display, Formatter};

use crate::common::df_schema::PyDFSchema;
use crate::errors::{py_type_err, DataFusionError};
use crate::expr::logical_node::LogicalNode;
use crate::expr::PyExpr;
use crate::sql::logical::PyLogicalPlan;

use super::py_expr_list;

#[pyclass(name = "Window", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyWindow {
    window: Window,
}

#[pyclass(name = "WindowFrame", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyWindowFrame {
    window_frame: WindowFrame,
}

#[pyclass(name = "WindowFrameBound", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyWindowFrameBound {
    frame_bound: WindowFrameBound,
}

impl From<PyWindow> for Window {
    fn from(window: PyWindow) -> Window {
        window.window
    }
}

impl From<Window> for PyWindow {
    fn from(window: Window) -> PyWindow {
        PyWindow { window }
    }
}

impl From<WindowFrame> for PyWindowFrame {
    fn from(window_frame: WindowFrame) -> Self {
        PyWindowFrame { window_frame }
    }
}

impl From<WindowFrameBound> for PyWindowFrameBound {
    fn from(frame_bound: WindowFrameBound) -> Self {
        PyWindowFrameBound { frame_bound }
    }
}

impl Display for PyWindow {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Over\n
            Window Expr: {:?}
            Schema: {:?}",
            &self.window.window_expr, &self.window.schema
        )
    }
}

#[pymethods]
impl PyWindow {
    /// Returns the schema of the Window
    pub fn schema(&self) -> PyResult<PyDFSchema> {
        Ok(self.window.schema.as_ref().clone().into())
    }

    /// Returns window expressions
    pub fn get_window_expr(&self) -> PyResult<Vec<PyExpr>> {
        py_expr_list(&self.window.window_expr)
    }

    /// Returns order by columns in a window function expression
    pub fn get_sort_exprs(&self, expr: PyExpr) -> PyResult<Vec<PyExpr>> {
        match expr.expr.unalias() {
            Expr::WindowFunction(WindowFunction { order_by, .. }) => py_expr_list(&order_by),
            other => Err(not_window_function_err(other)),
        }
    }

    /// Return partition by columns in a window function expression
    pub fn get_partition_exprs(&self, expr: PyExpr) -> PyResult<Vec<PyExpr>> {
        match expr.expr.unalias() {
            Expr::WindowFunction(WindowFunction { partition_by, .. }) => {
                py_expr_list(&partition_by)
            }
            other => Err(not_window_function_err(other)),
        }
    }

    /// Return input args for window function
    pub fn get_args(&self, expr: PyExpr) -> PyResult<Vec<PyExpr>> {
        match expr.expr.unalias() {
            Expr::WindowFunction(WindowFunction { args, .. }) => py_expr_list(&args),
            other => Err(not_window_function_err(other)),
        }
    }

    /// Return window function name
    pub fn window_func_name(&self, expr: PyExpr) -> PyResult<String> {
        match expr.expr.unalias() {
            Expr::WindowFunction(WindowFunction { fun, .. }) => Ok(fun.to_string()),
            other => Err(not_window_function_err(other)),
        }
    }

    /// Returns a Pywindow frame for a given window function expression
    pub fn get_window_frame(&self, expr: PyExpr) -> Option<PyWindowFrame> {
        match expr.expr.unalias() {
            Expr::WindowFunction(WindowFunction { window_frame, .. }) => Some(window_frame.into()),
            _ => None,
        }
    }
}

fn not_window_function_err(expr: Expr) -> PyErr {
    py_type_err(format!(
        "Provided {} Expr {:?} is not a WindowFunction type",
        expr.variant_name(),
        expr
    ))
}

#[pymethods]
impl PyWindowFrame {
    /// Returns the window frame units for the bounds
    pub fn get_frame_units(&self) -> PyResult<String> {
        Ok(self.window_frame.units.to_string())
    }
    /// Returns starting bound
    pub fn get_lower_bound(&self) -> PyResult<PyWindowFrameBound> {
        Ok(self.window_frame.start_bound.clone().into())
    }
    /// Returns end bound
    pub fn get_upper_bound(&self) -> PyResult<PyWindowFrameBound> {
        Ok(self.window_frame.end_bound.clone().into())
    }
}

#[pymethods]
impl PyWindowFrameBound {
    /// Returns if the frame bound is current row
    pub fn is_current_row(&self) -> bool {
        matches!(self.frame_bound, WindowFrameBound::CurrentRow)
    }

    /// Returns if the frame bound is preceding
    pub fn is_preceding(&self) -> bool {
        matches!(self.frame_bound, WindowFrameBound::Preceding(_))
    }

    /// Returns if the frame bound is following
    pub fn is_following(&self) -> bool {
        matches!(self.frame_bound, WindowFrameBound::Following(_))
    }
    /// Returns the offset of the window frame
    pub fn get_offset(&self) -> PyResult<Option<u64>> {
        match &self.frame_bound {
            WindowFrameBound::Preceding(val) | WindowFrameBound::Following(val) => match val {
                x if x.is_null() => Ok(None),
                ScalarValue::UInt64(v) => Ok(*v),
                // The cast below is only safe because window bounds cannot be negative
                ScalarValue::Int64(v) => Ok(v.map(|n| n as u64)),
                ScalarValue::Utf8(v) => {
                    let s = v.clone().unwrap();
                    match s.parse::<u64>() {
                        Ok(s) => Ok(Some(s)),
                        Err(_e) => Err(DataFusionError::Common(format!(
                            "Unable to parse u64 from Utf8 value '{s}'"
                        ))
                        .into()),
                    }
                }
                ref x => Err(DataFusionError::Common(format!(
                    "Unexpected window frame bound: {x}"
                ))
                .into()),
            },
            WindowFrameBound::CurrentRow => Ok(None),
        }
    }
    /// Returns if the frame bound is unbounded
    pub fn is_unbounded(&self) -> PyResult<bool> {
        match &self.frame_bound {
            WindowFrameBound::Preceding(v) | WindowFrameBound::Following(v) => Ok(v.is_null()),
            WindowFrameBound::CurrentRow => Ok(false),
        }
    }
}

impl LogicalNode for PyWindow {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![self.window.input.as_ref().clone().into()]
    }

    fn to_variant(&self, py: Python) -> PyResult<PyObject> {
        Ok(self.clone().into_py(py))
    }
}
