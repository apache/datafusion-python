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

use std::sync::Arc;

use datafusion::sql::unparser::dialect::{
    DefaultDialect, Dialect, DuckDBDialect, MySqlDialect, PostgreSqlDialect, SqliteDialect,
};
use pyo3::prelude::*;

#[pyclass(name = "Dialect", module = "datafusion.unparser", subclass)]
#[derive(Clone)]
pub struct PyDialect {
    pub dialect: Arc<dyn Dialect>,
}

#[pymethods]
impl PyDialect {
    #[staticmethod]
    pub fn default() -> Self {
        Self {
            dialect: Arc::new(DefaultDialect {}),
        }
    }
    #[staticmethod]
    pub fn postgres() -> Self {
        Self {
            dialect: Arc::new(PostgreSqlDialect {}),
        }
    }
    #[staticmethod]
    pub fn mysql() -> Self {
        Self {
            dialect: Arc::new(MySqlDialect {}),
        }
    }
    #[staticmethod]
    pub fn sqlite() -> Self {
        Self {
            dialect: Arc::new(SqliteDialect {}),
        }
    }
    #[staticmethod]
    pub fn duckdb() -> Self {
        Self {
            dialect: Arc::new(DuckDBDialect::new()),
        }
    }
}
