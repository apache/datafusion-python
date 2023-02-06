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
use pyo3::prelude::*;


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
#[pyclass(name = "DataTypeMap", module = "datafusion", subclass)]
pub struct DataTypeMap {
    arrow_type: PyDataType,
    python_type: PythonType,
    sql_type: SqlType,
}

impl DataTypeMap {
    fn new(arrow_type: DataType, python_type: PythonType, sql_type: SqlType) -> Self {
        DataTypeMap {
            arrow_type: PyDataType { data_type: arrow_type},
            python_type,
            sql_type
        }
    }

    pub fn map_from_arrow_type(arrow_type: &DataType) -> DataTypeMap {
        match arrow_type {
            DataType::Null => DataTypeMap::new(DataType::Null, PythonType::None, SqlType::NULL),
            DataType::Boolean => DataTypeMap::new(DataType::Boolean, PythonType::Bool, SqlType::BOOLEAN),
            DataType::Int8 => DataTypeMap::new(DataType::Int8, PythonType::Int, SqlType::TINYINT),
            DataType::Int16 => DataTypeMap::new(DataType::Int16, PythonType::Int, SqlType::SMALLINT),
            DataType::Int32 => DataTypeMap::new(DataType::Int32, PythonType::Int, SqlType::INTEGER),
            DataType::Int64 => DataTypeMap::new(DataType::Int64, PythonType::Int, SqlType::BIGINT),
            DataType::UInt8 => DataTypeMap::new(DataType::UInt8, PythonType::Int, SqlType::TINYINT),
            DataType::UInt16 => DataTypeMap::new(DataType::UInt16, PythonType::Int, SqlType::SMALLINT),
            DataType::UInt32 => DataTypeMap::new(DataType::UInt32, PythonType::Int, SqlType::INTEGER),
            DataType::UInt64 => DataTypeMap::new(DataType::UInt64, PythonType::Int, SqlType::BIGINT),
            DataType::Float16 => DataTypeMap::new(DataType::Float16, PythonType::Float, SqlType::FLOAT),
            DataType::Float32 => DataTypeMap::new(DataType::Float32, PythonType::Float, SqlType::FLOAT),
            DataType::Float64 => DataTypeMap::new(DataType::Float64, PythonType::Float, SqlType::FLOAT),
            DataType::Timestamp(_, _) => todo!(),
            DataType::Date32 => DataTypeMap::new(DataType::Date32, PythonType::Datetime, SqlType::DATE),
            DataType::Date64 => DataTypeMap::new(DataType::Date64, PythonType::Datetime, SqlType::DATE),
            DataType::Time32(_) => todo!(),
            DataType::Time64(_) => todo!(),
            DataType::Duration(_) => todo!(),
            DataType::Interval(_) => todo!(),
            DataType::Binary => DataTypeMap::new(DataType::Binary, PythonType::Bytes, SqlType::BINARY),
            DataType::FixedSizeBinary(_) => todo!(),
            DataType::LargeBinary => DataTypeMap::new(DataType::LargeBinary, PythonType::Bytes, SqlType::BINARY),
            DataType::Utf8 => DataTypeMap::new(DataType::Utf8, PythonType::Str, SqlType::VARCHAR),
            DataType::LargeUtf8 => DataTypeMap::new(DataType::LargeUtf8, PythonType::Str, SqlType::VARCHAR),
            DataType::List(_) => todo!(),
            DataType::FixedSizeList(_, _) => todo!(),
            DataType::LargeList(_) => todo!(),
            DataType::Struct(_) => todo!(),
            DataType::Union(_, _, _) => todo!(),
            DataType::Dictionary(_, _) => todo!(),
            DataType::Decimal128(_, _) => todo!(),
            DataType::Decimal256(_, _) => todo!(),
            DataType::Map(_, _) => todo!(),
        }
    }
}

#[pymethods]
impl DataTypeMap {

    #[new]
    pub fn py_new(arrow_type: PyDataType, python_type: PythonType, sql_type: SqlType) -> Self {
        DataTypeMap {
            arrow_type: arrow_type,
            python_type,
            sql_type
        }
    }
    
    #[staticmethod]
    #[pyo3(name = "arrow")]
    pub fn py_map_from_arrow_type(arrow_type: &PyDataType) -> PyResult<DataTypeMap> {
        Ok(DataTypeMap::map_from_arrow_type(&arrow_type.data_type))
    }

    #[staticmethod]
    #[pyo3(name = "sql")]
    pub fn map_from_sql_type(sql_type: &SqlType) -> PyResult<DataTypeMap> {
        Ok(match sql_type {
            SqlType::ANY => unimplemented!(),
            SqlType::ARRAY => todo!(), // unsure which type to use for DataType in this situation?
            SqlType::BIGINT => DataTypeMap::new(DataType::Int64, PythonType::Float, SqlType::BIGINT),
            SqlType::BINARY => DataTypeMap::new(DataType::Binary, PythonType::Bytes, SqlType::BINARY),
            SqlType::BOOLEAN => DataTypeMap::new(DataType::Boolean, PythonType::Bool, SqlType::BOOLEAN),
            SqlType::CHAR => DataTypeMap::new(DataType::UInt8, PythonType::Int, SqlType::CHAR),
            SqlType::COLUMN_LIST => unimplemented!(),
            SqlType::CURSOR => unimplemented!(),
            SqlType::DATE => DataTypeMap::new(DataType::Date64, PythonType::Datetime, SqlType::DATE),
            SqlType::DECIMAL => DataTypeMap::new(DataType::Decimal128(1, 1), PythonType::Float, SqlType::DECIMAL),
            SqlType::DISTINCT => unimplemented!(),
            SqlType::DOUBLE => DataTypeMap::new(DataType::Decimal256(1, 1), PythonType::Float, SqlType::DOUBLE),
            SqlType::DYNAMIC_STAR => unimplemented!(),
            SqlType::FLOAT => DataTypeMap::new(DataType::Decimal128(1, 1), PythonType::Float, SqlType::FLOAT),
            SqlType::GEOMETRY => unimplemented!(),
            SqlType::INTEGER => DataTypeMap::new(DataType::Int8, PythonType::Int, SqlType::INTEGER),
            SqlType::INTERVAL => todo!(),
            SqlType::INTERVAL_DAY => todo!(),
            SqlType::INTERVAL_DAY_HOUR => todo!(),
            SqlType::INTERVAL_DAY_MINUTE => todo!(),
            SqlType::INTERVAL_DAY_SECOND => todo!(),
            SqlType::INTERVAL_HOUR => todo!(),
            SqlType::INTERVAL_HOUR_MINUTE => todo!(),
            SqlType::INTERVAL_HOUR_SECOND => todo!(),
            SqlType::INTERVAL_MINUTE => todo!(),
            SqlType::INTERVAL_MINUTE_SECOND => todo!(),
            SqlType::INTERVAL_MONTH => todo!(),
            SqlType::INTERVAL_SECOND => todo!(),
            SqlType::INTERVAL_YEAR => todo!(),
            SqlType::INTERVAL_YEAR_MONTH => todo!(),
            SqlType::MAP => todo!(),
            SqlType::MULTISET => unimplemented!(),
            SqlType::NULL => DataTypeMap::new(DataType::Null, PythonType::None, SqlType::NULL),
            SqlType::OTHER => unimplemented!(),
            SqlType::REAL => todo!(),
            SqlType::ROW => todo!(),
            SqlType::SARG => unimplemented!(),
            SqlType::SMALLINT => DataTypeMap::new(DataType::Int16, PythonType::Int, SqlType::SMALLINT),
            SqlType::STRUCTURED => unimplemented!(),
            SqlType::SYMBOL => unimplemented!(),
            SqlType::TIME => todo!(),
            SqlType::TIME_WITH_LOCAL_TIME_ZONE => todo!(),
            SqlType::TIMESTAMP => todo!(),
            SqlType::TIMESTAMP_WITH_LOCAL_TIME_ZONE => todo!(),
            SqlType::TINYINT => DataTypeMap::new(DataType::Int8, PythonType::Int, SqlType::TINYINT),
            SqlType::UNKNOWN => unimplemented!(),
            SqlType::VARBINARY => DataTypeMap::new(DataType::LargeBinary, PythonType::Bytes, SqlType::VARBINARY),
            SqlType::VARCHAR => DataTypeMap::new(DataType::Utf8, PythonType::Str, SqlType::VARCHAR),
        })
    }
}


/// PyO3 requires that objects passed between Rust and Python implement the trait `PyClass`
/// Since `DataType` exists in another package we cannot make that happen here so we wrap 
/// `DataType` as `PyDataType` This exists solely to satisfy those constraints.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(name = "DataType", module = "datafusion")]
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
#[pyclass(name = "PythonType", module = "datafusion")]
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
#[pyclass(name = "SqlType", module = "datafusion")]
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
