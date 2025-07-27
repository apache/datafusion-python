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

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::sqlparser::ast::NullTreatment as DFNullTreatment;
use pyo3::{exceptions::PyValueError, prelude::*};

use crate::errors::py_datafusion_err;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct PyScalarValue(pub ScalarValue);

impl From<ScalarValue> for PyScalarValue {
    fn from(value: ScalarValue) -> Self {
        Self(value)
    }
}
impl From<PyScalarValue> for ScalarValue {
    fn from(value: PyScalarValue) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(eq, eq_int, name = "RexType", module = "datafusion.common")]
pub enum RexType {
    Alias,
    Literal,
    Call,
    Reference,
    ScalarSubquery,
    Other,
}

/// These bindings are tying together several disparate systems.
/// You have SQL types for the SQL strings and RDBMS systems itself.
/// Rust types for the DataFusion code
/// Arrow types which represents the underlying arrow format
/// Python types which represent the type in Python
/// It is important to keep all of those types in a single
/// and manageable location. Therefore this structure exists
/// to map those types and provide a simple place for developers
/// to map types from one system to another.
#[derive(Debug, Clone)]
#[pyclass(name = "DataTypeMap", module = "datafusion.common", subclass)]
pub struct DataTypeMap {
    #[pyo3(get, set)]
    pub arrow_type: PyDataType,
    #[pyo3(get, set)]
    pub python_type: PythonType,
    #[pyo3(get, set)]
    pub sql_type: SqlType,
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
            DataType::Timestamp(unit, tz) => Ok(DataTypeMap::new(
                DataType::Timestamp(*unit, tz.clone()),
                PythonType::Datetime,
                SqlType::DATE,
            )),
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
            DataType::Time32(unit) => Ok(DataTypeMap::new(
                DataType::Time32(*unit),
                PythonType::Datetime,
                SqlType::DATE,
            )),
            DataType::Time64(unit) => Ok(DataTypeMap::new(
                DataType::Time64(*unit),
                PythonType::Datetime,
                SqlType::DATE,
            )),
            DataType::Duration(_) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{arrow_type:?}"),
            ))),
            DataType::Interval(interval_unit) => Ok(DataTypeMap::new(
                DataType::Interval(*interval_unit),
                PythonType::Datetime,
                match interval_unit {
                    IntervalUnit::DayTime => SqlType::INTERVAL_DAY,
                    IntervalUnit::MonthDayNano => SqlType::INTERVAL_MONTH,
                    IntervalUnit::YearMonth => SqlType::INTERVAL_YEAR_MONTH,
                },
            )),
            DataType::Binary => Ok(DataTypeMap::new(
                DataType::Binary,
                PythonType::Bytes,
                SqlType::BINARY,
            )),
            DataType::FixedSizeBinary(_) => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{arrow_type:?}")),
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
                "{arrow_type:?}"
            )))),
            DataType::FixedSizeList(_, _) => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{arrow_type:?}")),
            )),
            DataType::LargeList(_) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{arrow_type:?}"),
            ))),
            DataType::Struct(_) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{arrow_type:?}"),
            ))),
            DataType::Union(_, _) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{arrow_type:?}"),
            ))),
            DataType::Dictionary(_, _) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{arrow_type:?}"),
            ))),
            DataType::Decimal128(precision, scale) => Ok(DataTypeMap::new(
                DataType::Decimal128(*precision, *scale),
                PythonType::Float,
                SqlType::DECIMAL,
            )),
            DataType::Decimal256(precision, scale) => Ok(DataTypeMap::new(
                DataType::Decimal256(*precision, *scale),
                PythonType::Float,
                SqlType::DECIMAL,
            )),
            DataType::Map(_, _) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{arrow_type:?}"),
            ))),
            DataType::RunEndEncoded(_, _) => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{arrow_type:?}")),
            )),
            DataType::BinaryView => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{arrow_type:?}"),
            ))),
            DataType::Utf8View => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{arrow_type:?}"
            )))),
            DataType::ListView(_) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{arrow_type:?}"),
            ))),
            DataType::LargeListView(_) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{arrow_type:?}"),
            ))),
        }
    }

    /// Generate the `DataTypeMap` from a `ScalarValue` instance
    pub fn map_from_scalar_value(scalar_val: &ScalarValue) -> Result<DataTypeMap, PyErr> {
        DataTypeMap::map_from_arrow_type(&DataTypeMap::map_from_scalar_to_arrow(scalar_val)?)
    }

    /// Maps a `ScalarValue` to an Arrow `DataType`
    pub fn map_from_scalar_to_arrow(scalar_val: &ScalarValue) -> Result<DataType, PyErr> {
        match scalar_val {
            ScalarValue::Boolean(_) => Ok(DataType::Boolean),
            ScalarValue::Float16(_) => Ok(DataType::Float16),
            ScalarValue::Float32(_) => Ok(DataType::Float32),
            ScalarValue::Float64(_) => Ok(DataType::Float64),
            ScalarValue::Decimal128(_, precision, scale) => {
                Ok(DataType::Decimal128(*precision, *scale))
            }
            ScalarValue::Decimal256(_, precision, scale) => {
                Ok(DataType::Decimal256(*precision, *scale))
            }
            ScalarValue::Dictionary(data_type, scalar_type) => {
                // Call this function again to map the dictionary scalar_value to an Arrow type
                Ok(DataType::Dictionary(
                    Box::new(*data_type.clone()),
                    Box::new(DataTypeMap::map_from_scalar_to_arrow(scalar_type)?),
                ))
            }
            ScalarValue::Int8(_) => Ok(DataType::Int8),
            ScalarValue::Int16(_) => Ok(DataType::Int16),
            ScalarValue::Int32(_) => Ok(DataType::Int32),
            ScalarValue::Int64(_) => Ok(DataType::Int64),
            ScalarValue::UInt8(_) => Ok(DataType::UInt8),
            ScalarValue::UInt16(_) => Ok(DataType::UInt16),
            ScalarValue::UInt32(_) => Ok(DataType::UInt32),
            ScalarValue::UInt64(_) => Ok(DataType::UInt64),
            ScalarValue::Utf8(_) => Ok(DataType::Utf8),
            ScalarValue::LargeUtf8(_) => Ok(DataType::LargeUtf8),
            ScalarValue::Binary(_) => Ok(DataType::Binary),
            ScalarValue::LargeBinary(_) => Ok(DataType::LargeBinary),
            ScalarValue::Date32(_) => Ok(DataType::Date32),
            ScalarValue::Date64(_) => Ok(DataType::Date64),
            ScalarValue::Time32Second(_) => Ok(DataType::Time32(TimeUnit::Second)),
            ScalarValue::Time32Millisecond(_) => Ok(DataType::Time32(TimeUnit::Millisecond)),
            ScalarValue::Time64Microsecond(_) => Ok(DataType::Time64(TimeUnit::Microsecond)),
            ScalarValue::Time64Nanosecond(_) => Ok(DataType::Time64(TimeUnit::Nanosecond)),
            ScalarValue::Null => Ok(DataType::Null),
            ScalarValue::TimestampSecond(_, tz) => {
                Ok(DataType::Timestamp(TimeUnit::Second, tz.to_owned()))
            }
            ScalarValue::TimestampMillisecond(_, tz) => {
                Ok(DataType::Timestamp(TimeUnit::Millisecond, tz.to_owned()))
            }
            ScalarValue::TimestampMicrosecond(_, tz) => {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, tz.to_owned()))
            }
            ScalarValue::TimestampNanosecond(_, tz) => {
                Ok(DataType::Timestamp(TimeUnit::Nanosecond, tz.to_owned()))
            }
            ScalarValue::IntervalYearMonth(..) => Ok(DataType::Interval(IntervalUnit::YearMonth)),
            ScalarValue::IntervalDayTime(..) => Ok(DataType::Interval(IntervalUnit::DayTime)),
            ScalarValue::IntervalMonthDayNano(..) => {
                Ok(DataType::Interval(IntervalUnit::MonthDayNano))
            }
            ScalarValue::List(arr) => Ok(arr.data_type().to_owned()),
            ScalarValue::Struct(_fields) => Err(py_datafusion_err(
                DataFusionError::NotImplemented("ScalarValue::Struct".to_string()),
            )),
            ScalarValue::FixedSizeBinary(size, _) => Ok(DataType::FixedSizeBinary(*size)),
            ScalarValue::FixedSizeList(_array_ref) => {
                // The FieldRef was removed from ScalarValue::FixedSizeList in
                // https://github.com/apache/arrow-datafusion/pull/8221, so we can no
                // longer convert back to a DataType here
                Err(py_datafusion_err(DataFusionError::NotImplemented(
                    "ScalarValue::FixedSizeList".to_string(),
                )))
            }
            ScalarValue::LargeList(_) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                "ScalarValue::LargeList".to_string(),
            ))),
            ScalarValue::DurationSecond(_) => Ok(DataType::Duration(TimeUnit::Second)),
            ScalarValue::DurationMillisecond(_) => Ok(DataType::Duration(TimeUnit::Millisecond)),
            ScalarValue::DurationMicrosecond(_) => Ok(DataType::Duration(TimeUnit::Microsecond)),
            ScalarValue::DurationNanosecond(_) => Ok(DataType::Duration(TimeUnit::Nanosecond)),
            ScalarValue::Union(_, _, _) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                "ScalarValue::LargeList".to_string(),
            ))),
            ScalarValue::Utf8View(_) => Ok(DataType::Utf8View),
            ScalarValue::BinaryView(_) => Ok(DataType::BinaryView),
            ScalarValue::Map(_) => Err(py_datafusion_err(DataFusionError::NotImplemented(
                "ScalarValue::Map".to_string(),
            ))),
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
    #[pyo3(name = "from_parquet_type_str")]
    /// When using pyarrow.parquet.read_metadata().schema.column(x).physical_type you are presented
    /// with a String type for schema rather than an object type. Here we make a best effort
    /// to convert that to a physical type.
    pub fn py_map_from_parquet_type_str(parquet_str_type: String) -> PyResult<DataTypeMap> {
        let arrow_dtype = match parquet_str_type.to_lowercase().as_str() {
            "boolean" => Ok(DataType::Boolean),
            "int32" => Ok(DataType::Int32),
            "int64" => Ok(DataType::Int64),
            "int96" => {
                // Int96 is an old parquet datatype that is now deprecated. We convert to nanosecond timestamp
                Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
            }
            "float" => Ok(DataType::Float32),
            "double" => Ok(DataType::Float64),
            "byte_array" => Ok(DataType::Utf8),
            _ => Err(PyValueError::new_err(format!(
                "Unable to determine Arrow Data Type from Parquet String type: {parquet_str_type:?}"
            ))),
        };
        DataTypeMap::map_from_arrow_type(&arrow_dtype?)
    }

    #[staticmethod]
    #[pyo3(name = "arrow")]
    pub fn py_map_from_arrow_type(arrow_type: &PyDataType) -> PyResult<DataTypeMap> {
        DataTypeMap::map_from_arrow_type(&arrow_type.data_type)
    }

    #[staticmethod]
    #[pyo3(name = "arrow_str")]
    pub fn py_map_from_arrow_type_str(arrow_type_str: String) -> PyResult<DataTypeMap> {
        let data_type = PyDataType::py_map_from_arrow_type_str(arrow_type_str);
        DataTypeMap::map_from_arrow_type(&data_type?.data_type)
    }

    #[staticmethod]
    #[pyo3(name = "sql")]
    pub fn py_map_from_sql_type(sql_type: &SqlType) -> PyResult<DataTypeMap> {
        match sql_type {
            SqlType::ANY => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{sql_type:?}"
            )))),
            SqlType::ARRAY => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{sql_type:?}"
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
                format!("{sql_type:?}"),
            ))),
            SqlType::CURSOR => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{sql_type:?}"
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
                "{sql_type:?}"
            )))),
            SqlType::DOUBLE => Ok(DataTypeMap::new(
                DataType::Decimal256(1, 1),
                PythonType::Float,
                SqlType::DOUBLE,
            )),
            SqlType::DYNAMIC_STAR => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{sql_type:?}"),
            ))),
            SqlType::FLOAT => Ok(DataTypeMap::new(
                DataType::Decimal128(1, 1),
                PythonType::Float,
                SqlType::FLOAT,
            )),
            SqlType::GEOMETRY => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{sql_type:?}"
            )))),
            SqlType::INTEGER => Ok(DataTypeMap::new(
                DataType::Int8,
                PythonType::Int,
                SqlType::INTEGER,
            )),
            SqlType::INTERVAL => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{sql_type:?}"
            )))),
            SqlType::INTERVAL_DAY => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{sql_type:?}"),
            ))),
            SqlType::INTERVAL_DAY_HOUR => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{sql_type:?}"),
            ))),
            SqlType::INTERVAL_DAY_MINUTE => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{sql_type:?}")),
            )),
            SqlType::INTERVAL_DAY_SECOND => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{sql_type:?}")),
            )),
            SqlType::INTERVAL_HOUR => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{sql_type:?}"),
            ))),
            SqlType::INTERVAL_HOUR_MINUTE => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{sql_type:?}")),
            )),
            SqlType::INTERVAL_HOUR_SECOND => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{sql_type:?}")),
            )),
            SqlType::INTERVAL_MINUTE => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{sql_type:?}"),
            ))),
            SqlType::INTERVAL_MINUTE_SECOND => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{sql_type:?}")),
            )),
            SqlType::INTERVAL_MONTH => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{sql_type:?}"),
            ))),
            SqlType::INTERVAL_SECOND => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{sql_type:?}"),
            ))),
            SqlType::INTERVAL_YEAR => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{sql_type:?}"),
            ))),
            SqlType::INTERVAL_YEAR_MONTH => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{sql_type:?}")),
            )),
            SqlType::MAP => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{sql_type:?}"
            )))),
            SqlType::MULTISET => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{sql_type:?}"
            )))),
            SqlType::NULL => Ok(DataTypeMap::new(
                DataType::Null,
                PythonType::None,
                SqlType::NULL,
            )),
            SqlType::OTHER => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{sql_type:?}"
            )))),
            SqlType::REAL => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{sql_type:?}"
            )))),
            SqlType::ROW => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{sql_type:?}"
            )))),
            SqlType::SARG => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{sql_type:?}"
            )))),
            SqlType::SMALLINT => Ok(DataTypeMap::new(
                DataType::Int16,
                PythonType::Int,
                SqlType::SMALLINT,
            )),
            SqlType::STRUCTURED => Err(py_datafusion_err(DataFusionError::NotImplemented(
                format!("{sql_type:?}"),
            ))),
            SqlType::SYMBOL => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{sql_type:?}"
            )))),
            SqlType::TIME => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{sql_type:?}"
            )))),
            SqlType::TIME_WITH_LOCAL_TIME_ZONE => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{sql_type:?}")),
            )),
            SqlType::TIMESTAMP => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{sql_type:?}"
            )))),
            SqlType::TIMESTAMP_WITH_LOCAL_TIME_ZONE => Err(py_datafusion_err(
                DataFusionError::NotImplemented(format!("{sql_type:?}")),
            )),
            SqlType::TINYINT => Ok(DataTypeMap::new(
                DataType::Int8,
                PythonType::Int,
                SqlType::TINYINT,
            )),
            SqlType::UNKNOWN => Err(py_datafusion_err(DataFusionError::NotImplemented(format!(
                "{sql_type:?}"
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

    /// Unfortunately PyO3 does not allow for us to expose the DataType as an enum since
    /// we cannot directly annotae the Enum instance of dependency code. Therefore, here
    /// we provide an enum to mimic it.
    #[pyo3(name = "friendly_arrow_type_name")]
    pub fn friendly_arrow_type_name(&self) -> PyResult<&str> {
        Ok(match &self.arrow_type.data_type {
            DataType::Null => "Null",
            DataType::Boolean => "Boolean",
            DataType::Int8 => "Int8",
            DataType::Int16 => "Int16",
            DataType::Int32 => "Int32",
            DataType::Int64 => "Int64",
            DataType::UInt8 => "UInt8",
            DataType::UInt16 => "UInt16",
            DataType::UInt32 => "UInt32",
            DataType::UInt64 => "UInt64",
            DataType::Float16 => "Float16",
            DataType::Float32 => "Float32",
            DataType::Float64 => "Float64",
            DataType::Timestamp(_, _) => "Timestamp",
            DataType::Date32 => "Date32",
            DataType::Date64 => "Date64",
            DataType::Time32(_) => "Time32",
            DataType::Time64(_) => "Time64",
            DataType::Duration(_) => "Duration",
            DataType::Interval(_) => "Interval",
            DataType::Binary => "Binary",
            DataType::FixedSizeBinary(_) => "FixedSizeBinary",
            DataType::LargeBinary => "LargeBinary",
            DataType::Utf8 => "Utf8",
            DataType::LargeUtf8 => "LargeUtf8",
            DataType::List(_) => "List",
            DataType::FixedSizeList(_, _) => "FixedSizeList",
            DataType::LargeList(_) => "LargeList",
            DataType::Struct(_) => "Struct",
            DataType::Union(_, _) => "Union",
            DataType::Dictionary(_, _) => "Dictionary",
            DataType::Decimal128(_, _) => "Decimal128",
            DataType::Decimal256(_, _) => "Decimal256",
            DataType::Map(_, _) => "Map",
            DataType::RunEndEncoded(_, _) => "RunEndEncoded",
            DataType::BinaryView => "BinaryView",
            DataType::Utf8View => "Utf8View",
            DataType::ListView(_) => "ListView",
            DataType::LargeListView(_) => "LargeListView",
        })
    }
}

/// PyO3 requires that objects passed between Rust and Python implement the trait `PyClass`
/// Since `DataType` exists in another package we cannot make that happen here so we wrap
/// `DataType` as `PyDataType` This exists solely to satisfy those constraints.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(name = "DataType", module = "datafusion.common")]
pub struct PyDataType {
    pub data_type: DataType,
}

impl PyDataType {
    /// There are situations when obtaining dtypes on the Python side where the Arrow type
    /// is presented as a String rather than an actual DataType. This function is used to
    /// convert that String to a DataType for the Python side to use.
    pub fn py_map_from_arrow_type_str(arrow_str_type: String) -> PyResult<PyDataType> {
        // Certain string types contain "metadata" that should be trimmed here. Ex: "datetime64[ns, Europe/Berlin]"
        let arrow_str_type = match arrow_str_type.find('[') {
            Some(index) => arrow_str_type[0..index].to_string(),
            None => arrow_str_type, // Return early if ',' is not found.
        };

        let arrow_dtype = match arrow_str_type.to_lowercase().as_str() {
            "bool" => Ok(DataType::Boolean),
            "boolean" => Ok(DataType::Boolean),
            "uint8" => Ok(DataType::UInt8),
            "uint16" => Ok(DataType::UInt16),
            "uint32" => Ok(DataType::UInt32),
            "uint64" => Ok(DataType::UInt64),
            "int8" => Ok(DataType::Int8),
            "int16" => Ok(DataType::Int16),
            "int32" => Ok(DataType::Int32),
            "int64" => Ok(DataType::Int64),
            "float" => Ok(DataType::Float32),
            "double" => Ok(DataType::Float64),
            "float16" => Ok(DataType::Float16),
            "float32" => Ok(DataType::Float32),
            "float64" => Ok(DataType::Float64),
            "datetime64" => Ok(DataType::Date64),
            "object" => Ok(DataType::Utf8),
            _ => Err(PyValueError::new_err(format!(
                "Unable to determine Arrow Data Type from Arrow String type: {arrow_str_type:?}"
            ))),
        };
        Ok(PyDataType {
            data_type: arrow_dtype?,
        })
    }
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
#[pyclass(eq, eq_int, name = "PythonType", module = "datafusion.common")]
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
#[pyclass(eq, eq_int, name = "SqlType", module = "datafusion.common")]
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

/// Specifies Ignore / Respect NULL within window functions.
/// For example
/// `FIRST_VALUE(column2) IGNORE NULLS OVER (PARTITION BY column1)`
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(eq, eq_int, name = "NullTreatment", module = "datafusion.common")]
pub enum NullTreatment {
    IGNORE_NULLS,
    RESPECT_NULLS,
}

impl From<NullTreatment> for DFNullTreatment {
    fn from(null_treatment: NullTreatment) -> DFNullTreatment {
        match null_treatment {
            NullTreatment::IGNORE_NULLS => DFNullTreatment::IgnoreNulls,
            NullTreatment::RESPECT_NULLS => DFNullTreatment::RespectNulls,
        }
    }
}

impl From<DFNullTreatment> for NullTreatment {
    fn from(null_treatment: DFNullTreatment) -> NullTreatment {
        match null_treatment {
            DFNullTreatment::IgnoreNulls => NullTreatment::IGNORE_NULLS,
            DFNullTreatment::RespectNulls => NullTreatment::RESPECT_NULLS,
        }
    }
}
