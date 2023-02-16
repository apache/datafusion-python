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

use datafusion::arrow::datatypes::DataType;
use datafusion_common::DataFusionError;
use pyo3::prelude::*;

use crate::errors::py_datafusion_err;

/// These bindings are tying together several disparate systems.
/// You have SQL types for the SQL strings and RDBMS systems itself.
/// Rust types for the DataFusion code
/// Arrow types which represents the underlying arrow format
/// Python types which represent the type in Python
/// It is important to keep all of those types in a single
/// and managable location. Therefore this structure exists
/// to map those types and provide a simple place for developers
/// to map types from one system to another.
#[derive(Debug, Clone)]
#[pyclass(name = "DataTypeMap", module = "datafusion.common", subclass)]
pub struct DataTypeMap {
    #[allow(dead_code)]
    arrow_type: PyDataType,
    #[allow(dead_code)]
    python_type: PythonType,
    #[allow(dead_code)]
    sql_type: SqlType,
}

impl DataTypeMap {
    fn new(arrow_type: DataType, python_type: PythonType, sql_type: SqlType) -> Self {
        DataTypeMap {
            arrow_type: PyDataType {
                data_type: arrow_type,
            },
            python_type,
            sql_type,
        }
    }

    pub fn map_from_arrow_type(arrow_type: &DataType) -> Result<DataTypeMap, PyErr> {
        match arrow_type {
            DataType::Null => Ok(DataTypeMap::new(
                DataType::Null,
                PythonType::None,
                SqlType::NULL,
            )),
            DataType::Boolean => Ok(DataTypeMap::new(
                DataType::Boolean,
                PythonType::Bool,
                SqlType::BOOLEAN,
            )),
            DataType::Int8 => Ok(DataTypeMap::new(
                DataType::Int8,
                PythonType::Int,
                SqlType::TINYINT,
            )),
            DataType::Int16 => Ok(DataTypeMap::new(
                DataType::Int16,
                PythonType::Int,
                SqlType::SMALLINT,
            )),
            DataType::Int32 => Ok(DataTypeMap::new(
                DataType::Int32,
                PythonType::Int,
                SqlType::INTEGER,
            )),
            DataType::Int64 => Ok(DataTypeMap::new(
                DataType::Int64,
                PythonType::Int,
                SqlType::BIGINT,
            )),
            DataType::UInt8 => Ok(DataTypeMap::new(
                DataType::UInt8,
                PythonType::Int,
                SqlType::TINYINT,
            )),
            DataType::UInt16 => Ok(DataTypeMap::new(
                DataType::UInt16,
                PythonType::Int,
                SqlType::SMALLINT,
            )),
            DataType::UInt32 => Ok(DataTypeMap::new(
                DataType::UInt32,
                PythonType::Int,
                SqlType::INTEGER,
            )),
            DataType::UInt64 => Ok(DataTypeMap::new(
                DataType::UInt64,
                PythonType::Int,
                SqlType::BIGINT,
            )),
            DataType::Float16 => Ok(DataTypeMap::new(
                DataType::Float16,
                PythonType::Float,
                SqlType::FLOAT,
            )),
            DataType::Float32 => Ok(DataTypeMap::new(
                DataType::Float32,
                PythonType::Float,
                SqlType::FLOAT,
            )),
            DataType::Float64 => Ok(DataTypeMap::new(
                DataType::Float64,
                PythonType::Float,
                SqlType::FLOAT,
            )),
            DataType::Timestamp(_, _) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", arrow_type),
            ))),
            DataType::Date32 => Ok(DataTypeMap::new(
                DataType::Date32,
                PythonType::Datetime,
                SqlType::DATE,
            )),
            DataType::Date64 => Ok(DataTypeMap::new(
                DataType::Date64,
                PythonType::Datetime,
                SqlType::DATE,
            )),
            DataType::Time32(_) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", arrow_type),
            ))),
            DataType::Time64(_) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", arrow_type),
            ))),
            DataType::Duration(_) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", arrow_type),
            ))),
            DataType::Interval(_) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", arrow_type),
            ))),
            DataType::Binary => Ok(DataTypeMap::new(
                DataType::Binary,
                PythonType::Bytes,
                SqlType::BINARY,
            )),
            DataType::FixedSizeBinary(_) => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{:?}", arrow_type)),
            )),
            DataType::LargeBinary => Ok(DataTypeMap::new(
                DataType::LargeBinary,
                PythonType::Bytes,
                SqlType::BINARY,
            )),
            DataType::Utf8 => Ok(DataTypeMap::new(
                DataType::Utf8,
                PythonType::Str,
                SqlType::VARCHAR,
            )),
            DataType::LargeUtf8 => Ok(DataTypeMap::new(
                DataType::LargeUtf8,
                PythonType::Str,
                SqlType::VARCHAR,
            )),
            DataType::List(_) => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                arrow_type
            )))),
            DataType::FixedSizeList(_, _) => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{:?}", arrow_type)),
            )),
            DataType::LargeList(_) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", arrow_type),
            ))),
            DataType::Struct(_) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", arrow_type),
            ))),
            DataType::Union(_, _, _) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", arrow_type),
            ))),
            DataType::Dictionary(_, _) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", arrow_type),
            ))),
            DataType::Decimal128(_, _) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", arrow_type),
            ))),
            DataType::Decimal256(_, _) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", arrow_type),
            ))),
            DataType::Map(_, _) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", arrow_type),
            ))),
            DataType::RunEndEncoded(_, _) => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{:?}", arrow_type)),
            )),
        }
    }
}

#[pymethods]
impl DataTypeMap {
    #[new]
    pub fn py_new(arrow_type: PyDataType, python_type: PythonType, sql_type: SqlType) -> Self {
        DataTypeMap {
            arrow_type,
            python_type,
            sql_type,
        }
    }

    #[staticmethod]
    #[pyo3(name = "arrow")]
    pub fn py_map_from_arrow_type(arrow_type: &PyDataType) -> PyResult<DataTypeMap> {
        DataTypeMap::map_from_arrow_type(&arrow_type.data_type)
    }

    #[staticmethod]
    #[pyo3(name = "sql")]
    pub fn py_map_from_sql_type(sql_type: &SqlType) -> PyResult<DataTypeMap> {
        match sql_type {
            SqlType::ANY => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::ARRAY => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::BIGINT => Ok(DataTypeMap::new(
                DataType::Int64,
                PythonType::Int,
                SqlType::BIGINT,
            )),
            SqlType::BINARY => Ok(DataTypeMap::new(
                DataType::Binary,
                PythonType::Bytes,
                SqlType::BINARY,
            )),
            SqlType::BOOLEAN => Ok(DataTypeMap::new(
                DataType::Boolean,
                PythonType::Bool,
                SqlType::BOOLEAN,
            )),
            SqlType::CHAR => Ok(DataTypeMap::new(
                DataType::UInt8,
                PythonType::Int,
                SqlType::CHAR,
            )),
            SqlType::COLUMN_LIST => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", sql_type),
            ))),
            SqlType::CURSOR => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::DATE => Ok(DataTypeMap::new(
                DataType::Date64,
                PythonType::Datetime,
                SqlType::DATE,
            )),
            SqlType::DECIMAL => Ok(DataTypeMap::new(
                DataType::Decimal128(1, 1),
                PythonType::Float,
                SqlType::DECIMAL,
            )),
            SqlType::DISTINCT => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::DOUBLE => Ok(DataTypeMap::new(
                DataType::Decimal256(1, 1),
                PythonType::Float,
                SqlType::DOUBLE,
            )),
            SqlType::DYNAMIC_STAR => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", sql_type),
            ))),
            SqlType::FLOAT => Ok(DataTypeMap::new(
                DataType::Decimal128(1, 1),
                PythonType::Float,
                SqlType::FLOAT,
            )),
            SqlType::GEOMETRY => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::INTEGER => Ok(DataTypeMap::new(
                DataType::Int8,
                PythonType::Int,
                SqlType::INTEGER,
            )),
            SqlType::INTERVAL => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::INTERVAL_DAY => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", sql_type),
            ))),
            SqlType::INTERVAL_DAY_HOUR => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", sql_type),
            ))),
            SqlType::INTERVAL_DAY_MINUTE => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{:?}", sql_type)),
            )),
            SqlType::INTERVAL_DAY_SECOND => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{:?}", sql_type)),
            )),
            SqlType::INTERVAL_HOUR => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", sql_type),
            ))),
            SqlType::INTERVAL_HOUR_MINUTE => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{:?}", sql_type)),
            )),
            SqlType::INTERVAL_HOUR_SECOND => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{:?}", sql_type)),
            )),
            SqlType::INTERVAL_MINUTE => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", sql_type),
            ))),
            SqlType::INTERVAL_MINUTE_SECOND => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{:?}", sql_type)),
            )),
            SqlType::INTERVAL_MONTH => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", sql_type),
            ))),
            SqlType::INTERVAL_SECOND => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", sql_type),
            ))),
            SqlType::INTERVAL_YEAR => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", sql_type),
            ))),
            SqlType::INTERVAL_YEAR_MONTH => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{:?}", sql_type)),
            )),
            SqlType::MAP => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::MULTISET => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::NULL => Ok(DataTypeMap::new(
                DataType::Null,
                PythonType::None,
                SqlType::NULL,
            )),
            SqlType::OTHER => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::REAL => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::ROW => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::SARG => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::SMALLINT => Ok(DataTypeMap::new(
                DataType::Int16,
                PythonType::Int,
                SqlType::SMALLINT,
            )),
            SqlType::STRUCTURED => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{:?}", sql_type),
            ))),
            SqlType::SYMBOL => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::TIME => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::TIME_WITH_LOCAL_TIME_ZONE => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{:?}", sql_type)),
            )),
            SqlType::TIMESTAMP => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::TIMESTAMP_WITH_LOCAL_TIME_ZONE => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{:?}", sql_type)),
            )),
            SqlType::TINYINT => Ok(DataTypeMap::new(
                DataType::Int8,
                PythonType::Int,
                SqlType::TINYINT,
            )),
            SqlType::UNKNOWN => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{:?}",
                sql_type
            )))),
            SqlType::VARBINARY => Ok(DataTypeMap::new(
                DataType::LargeBinary,
                PythonType::Bytes,
                SqlType::VARBINARY,
            )),
            SqlType::VARCHAR => Ok(DataTypeMap::new(
                DataType::Utf8,
                PythonType::Str,
                SqlType::VARCHAR,
            )),
        }
    }
}

/// PyO3 requires that objects passed between Rust and Python implement the trait `PyClass`
/// Since `DataType` exists in another package we cannot make that happen here so we wrap
/// `DataType` as `PyDataType` This exists solely to satisfy those constraints.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(name = "DataType", module = "datafusion.common")]
pub struct PyDataType {
    data_type: DataType,
}

impl From<PyDataType> for DataType {
    fn from(data_type: PyDataType) -> DataType {
        data_type.data_type
    }
}

impl From<DataType> for PyDataType {
    fn from(data_type: DataType) -> PyDataType {
        PyDataType { data_type }
    }
}

/// Represents the possible Python types that can be mapped to the SQL types
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(name = "PythonType", module = "datafusion.common")]
pub enum PythonType {
    Array,
    Bool,
    Bytes,
    Datetime,
    Float,
    Int,
    List,
    None,
    Object,
    Str,
}

/// Represents the types that are possible for DataFusion to parse
/// from a SQL query. Aka "SqlType" and are valid values for
/// ANSI SQL
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(name = "SqlType", module = "datafusion.common")]
pub enum SqlType {
    ANY,
    ARRAY,
    BIGINT,
    BINARY,
    BOOLEAN,
    CHAR,
    COLUMN_LIST,
    CURSOR,
    DATE,
    DECIMAL,
    DISTINCT,
    DOUBLE,
    DYNAMIC_STAR,
    FLOAT,
    GEOMETRY,
    INTEGER,
    INTERVAL,
    INTERVAL_DAY,
    INTERVAL_DAY_HOUR,
    INTERVAL_DAY_MINUTE,
    INTERVAL_DAY_SECOND,
    INTERVAL_HOUR,
    INTERVAL_HOUR_MINUTE,
    INTERVAL_HOUR_SECOND,
    INTERVAL_MINUTE,
    INTERVAL_MINUTE_SECOND,
    INTERVAL_MONTH,
    INTERVAL_SECOND,
    INTERVAL_YEAR,
    INTERVAL_YEAR_MONTH,
    MAP,
    MULTISET,
    NULL,
    OTHER,
    REAL,
    ROW,
    SARG,
    SMALLINT,
    STRUCTURED,
    SYMBOL,
    TIME,
    TIME_WITH_LOCAL_TIME_ZONE,
    TIMESTAMP,
    TIMESTAMP_WITH_LOCAL_TIME_ZONE,
    TINYINT,
    UNKNOWN,
    VARBINARY,
    VARCHAR,
}
