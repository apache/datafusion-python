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

#[pymethods]
impl DataTypeMap {
    
    #[staticmethod]
    #[pyo3(name = "arrow")]
    pub fn map_from_arrow_type(arrow_type: &PyDataType) -> PyResult<DataTypeMap> {
        match arrow_type.data_type {
            DataType::Null => todo!(),
            DataType::Boolean => todo!(),
            DataType::Int8 => todo!(),
            DataType::Int16 => todo!(),
            DataType::Int32 => todo!(),
            DataType::Int64 => todo!(),
            DataType::UInt8 => todo!(),
            DataType::UInt16 => todo!(),
            DataType::UInt32 => todo!(),
            DataType::UInt64 => todo!(),
            DataType::Float16 => todo!(),
            DataType::Float32 => todo!(),
            DataType::Float64 => todo!(),
            DataType::Timestamp(_, _) => todo!(),
            DataType::Date32 => todo!(),
            DataType::Date64 => todo!(),
            DataType::Time32(_) => todo!(),
            DataType::Time64(_) => todo!(),
            DataType::Duration(_) => todo!(),
            DataType::Interval(_) => todo!(),
            DataType::Binary => todo!(),
            DataType::FixedSizeBinary(_) => todo!(),
            DataType::LargeBinary => todo!(),
            DataType::Utf8 => todo!(),
            DataType::LargeUtf8 => todo!(),
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

    #[staticmethod]
    #[pyo3(name = "sql")]
    pub fn map_from_sql_type(sql_type: &SqlType) -> PyResult<DataTypeMap> {

        let data_type: DataTypeMap = match sql_type {
            SqlType::ANY => unimplemented!(),
            SqlType::ARRAY => todo!(), // unsure which type to use for DataType in this situation?
            SqlType::BIGINT => DataTypeMap {
                arrow_type: PyDataType { data_type: DataType::Int64 },
                python_type: PythonType::Float64, // According to https://learn.microsoft.com/en-us/sql/machine-learning/python/python-libraries-and-data-types?view=sql-server-ver16 should be float
                sql_type: SqlType::BIGINT
            },
            SqlType::BINARY => DataTypeMap {
                arrow_type: PyDataType { data_type: DataType::Binary },
                python_type: PythonType::Bytes,
                sql_type: SqlType::BINARY
            },
            SqlType::BOOLEAN => DataTypeMap {
                arrow_type: PyDataType { data_type: DataType::Boolean },
                python_type: PythonType::Bool,
                sql_type: SqlType::BOOLEAN
            },
            SqlType::CHAR => DataTypeMap {
                arrow_type: PyDataType { data_type: DataType::UInt8 },
                python_type: PythonType::Int32,
                sql_type: SqlType::CHAR
            },
            SqlType::COLUMN_LIST => unimplemented!(),
            SqlType::CURSOR => unimplemented!(),
            SqlType::DATE => DataTypeMap {
                arrow_type: PyDataType { data_type: DataType::Date64 },
                python_type: PythonType::Datetime,
                sql_type: SqlType::DATE
            },
            SqlType::DECIMAL => todo!(),
            SqlType::DISTINCT => unimplemented!(),
            SqlType::DOUBLE => todo!(),
            SqlType::DYNAMIC_STAR => unimplemented!(),
            SqlType::FLOAT => todo!(),
            SqlType::GEOMETRY => unimplemented!(),
            SqlType::INTEGER => DataTypeMap {
                arrow_type: PyDataType { data_type: DataType::Int8 },
                python_type: PythonType::Int32,
                sql_type: SqlType::INTEGER
            },
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
            SqlType::NULL => todo!(),
            SqlType::OTHER => unimplemented!(),
            SqlType::REAL => todo!(),
            SqlType::ROW => todo!(),
            SqlType::SARG => unimplemented!(),
            SqlType::SMALLINT => todo!(),
            SqlType::STRUCTURED => unimplemented!(),
            SqlType::SYMBOL => unimplemented!(),
            SqlType::TIME => todo!(),
            SqlType::TIME_WITH_LOCAL_TIME_ZONE => todo!(),
            SqlType::TIMESTAMP => todo!(),
            SqlType::TIMESTAMP_WITH_LOCAL_TIME_ZONE => todo!(),
            SqlType::TINYINT => todo!(),
            SqlType::UNKNOWN => todo!(),
            SqlType::VARBINARY => todo!(),
            SqlType::VARCHAR => DataTypeMap {
                arrow_type: PyDataType { data_type: DataType::Utf8 },
                python_type: PythonType::Str,
                sql_type: SqlType::VARCHAR
            },
        };

        Ok(data_type)
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
    Float64,
    Int32,
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
