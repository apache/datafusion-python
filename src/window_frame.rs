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

use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::window_frame::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use pyo3::prelude::*;
use std::fmt::{Display, Formatter};

use crate::errors::py_datafusion_err;

#[pyclass(name = "WindowFrame", module = "datafusion", subclass)]
#[derive(Clone)]
pub struct PyWindowFrame {
    frame: WindowFrame,
}

impl From<PyWindowFrame> for WindowFrame {
    fn from(frame: PyWindowFrame) -> Self {
        frame.frame
    }
}

impl From<WindowFrame> for PyWindowFrame {
    fn from(frame: WindowFrame) -> PyWindowFrame {
        PyWindowFrame { frame }
    }
}

impl Display for PyWindowFrame {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "OVER ({} BETWEEN {} AND {})",
            self.frame.units, self.frame.start_bound, self.frame.end_bound
        )
    }
}

#[pymethods]
impl PyWindowFrame {
    #[new(unit, start_bound, end_bound)]
    pub fn new(units: &str, start_bound: Option<u64>, end_bound: Option<u64>) -> PyResult<Self> {
        let units = units.to_ascii_lowercase();
        let units = match units.as_str() {
            "rows" => WindowFrameUnits::Rows,
            "range" => WindowFrameUnits::Range,
            "groups" => WindowFrameUnits::Groups,
            _ => {
                return Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                    "{:?}",
                    units,
                ))));
            }
        };
        let start_bound = match start_bound {
            Some(start_bound) => {
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(start_bound)))
            }
            None => match units {
                WindowFrameUnits::Range => WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameUnits::Rows => WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameUnits::Groups => {
                    return Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                        "{:?}",
                        units,
                    ))));
                }
            },
        };
        let end_bound = match end_bound {
            Some(end_bound) => WindowFrameBound::Following(ScalarValue::UInt64(Some(end_bound))),
            None => match units {
                WindowFrameUnits::Rows => WindowFrameBound::Following(ScalarValue::UInt64(None)),
                WindowFrameUnits::Range => WindowFrameBound::Following(ScalarValue::UInt64(None)),
                WindowFrameUnits::Groups => {
                    return Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                        "{:?}",
                        units,
                    ))));
                }
            },
        };
        Ok(PyWindowFrame {
            frame: WindowFrame {
                units,
                start_bound,
                end_bound,
            },
        })
    }

    /// Get a String representation of this window frame
    fn __repr__(&self) -> String {
        format!("{}", self)
    }
}
