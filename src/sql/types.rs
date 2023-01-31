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

pub mod data_type;
pub mod sql_type;

use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion_sql::sqlparser::{ast::DataType as SQLType, parser::Parser, tokenizer::Tokenizer};
use pyo3::{prelude::*, types::PyDict};

use self::sql_type::SqlType;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(name = "RexType", module = "datafusion")]
pub enum RexType {
    Literal,
    Call,
    Reference,
    SubqueryAlias,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(name = "DataTypeMap", module = "datafusion", subclass)]
pub struct DataTypeMap {
    sql_type: SqlType,
    arrow_type: DataType,
}


// /// Functions not exposed to Python
// impl DaskTypeMap {
//     pub fn from(sql_type: SqlTypeName, data_type: PyDataType) -> Self {
//         DaskTypeMap {
//             sql_type,
//             data_type,
//         }
//     }
// }

// #[pymethods]
// impl DaskTypeMap {
//     #[new]
//     #[pyo3(signature = (sql_type, **py_kwargs))]
//     fn new(sql_type: SqlTypeName, py_kwargs: Option<&PyDict>) -> PyResult<Self> {
//         let d_type: DataType = match sql_type {
//             SqlTypeName::TIMESTAMP_WITH_LOCAL_TIME_ZONE => {
//                 let (unit, tz) = match py_kwargs {
//                     Some(dict) => {
//                         let tz: Option<String> = match dict.get_item("tz") {
//                             Some(e) => {
//                                 let res: PyResult<String> = e.extract();
//                                 Some(res.unwrap())
//                             }
//                             None => None,
//                         };
//                         let unit: TimeUnit = match dict.get_item("unit") {
//                             Some(e) => {
//                                 let res: PyResult<&str> = e.extract();
//                                 match res.unwrap() {
//                                     "Second" => TimeUnit::Second,
//                                     "Millisecond" => TimeUnit::Millisecond,
//                                     "Microsecond" => TimeUnit::Microsecond,
//                                     "Nanosecond" => TimeUnit::Nanosecond,
//                                     _ => TimeUnit::Nanosecond,
//                                 }
//                             }
//                             // Default to Nanosecond which is common if not present
//                             None => TimeUnit::Nanosecond,
//                         };
//                         (unit, tz)
//                     }
//                     // Default to Nanosecond and None for tz which is common if not present
//                     None => (TimeUnit::Nanosecond, None),
//                 };
//                 DataType::Timestamp(unit, tz)
//             }
//             SqlTypeName::TIMESTAMP => {
//                 let (unit, tz) = match py_kwargs {
//                     Some(dict) => {
//                         let tz: Option<String> = match dict.get_item("tz") {
//                             Some(e) => {
//                                 let res: PyResult<String> = e.extract();
//                                 Some(res.unwrap())
//                             }
//                             None => None,
//                         };
//                         let unit: TimeUnit = match dict.get_item("unit") {
//                             Some(e) => {
//                                 let res: PyResult<&str> = e.extract();
//                                 match res.unwrap() {
//                                     "Second" => TimeUnit::Second,
//                                     "Millisecond" => TimeUnit::Millisecond,
//                                     "Microsecond" => TimeUnit::Microsecond,
//                                     "Nanosecond" => TimeUnit::Nanosecond,
//                                     _ => TimeUnit::Nanosecond,
//                                 }
//                             }
//                             // Default to Nanosecond which is common if not present
//                             None => TimeUnit::Nanosecond,
//                         };
//                         (unit, tz)
//                     }
//                     // Default to Nanosecond and None for tz which is common if not present
//                     None => (TimeUnit::Nanosecond, None),
//                 };
//                 DataType::Timestamp(unit, tz)
//             }
//             _ => sql_type.to_arrow()?,
//         };

//         Ok(DaskTypeMap {
//             sql_type,
//             data_type: d_type.into(),
//         })
//     }

//     #[pyo3(name = "getSqlType")]
//     pub fn sql_type(&self) -> SqlTypeName {
//         self.sql_type.clone()
//     }

//     #[pyo3(name = "getDataType")]
//     pub fn data_type(&self) -> PyDataType {
//         self.data_type.clone()
//     }
// }

// #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
// #[pyclass(name = "PyDataType", module = "datafusion", subclass)]
// pub struct PyDataType {
//     data_type: DataType,
// }

// impl From<PyDataType> for DataType {
//     fn from(data_type: PyDataType) -> DataType {
//         data_type.data_type
//     }
// }

// impl From<DataType> for PyDataType {
//     fn from(data_type: DataType) -> PyDataType {
//         PyDataType { data_type }
//     }
// }


// impl SqlTypeName {
//     pub fn to_arrow(&self) -> Result<DataType, DaskPlannerError> {
//         match self {
//             SqlTypeName::NULL => Ok(DataType::Null),
//             SqlTypeName::BOOLEAN => Ok(DataType::Boolean),
//             SqlTypeName::TINYINT => Ok(DataType::Int8),
//             SqlTypeName::SMALLINT => Ok(DataType::Int16),
//             SqlTypeName::INTEGER => Ok(DataType::Int32),
//             SqlTypeName::BIGINT => Ok(DataType::Int64),
//             SqlTypeName::REAL => Ok(DataType::Float16),
//             SqlTypeName::FLOAT => Ok(DataType::Float32),
//             SqlTypeName::DOUBLE => Ok(DataType::Float64),
//             SqlTypeName::DATE => Ok(DataType::Date64),
//             SqlTypeName::VARCHAR => Ok(DataType::Utf8),
//             _ => Err(DaskPlannerError::Internal(format!(
//                 "Cannot determine Arrow type for Dask SQL type '{:?}'",
//                 self
//             ))),
//         }
//     }

//     pub fn from_arrow(arrow_type: &DataType) -> Result<Self, DaskPlannerError> {
//         match arrow_type {
//             DataType::Null => Ok(SqlTypeName::NULL),
//             DataType::Boolean => Ok(SqlTypeName::BOOLEAN),
//             DataType::Int8 => Ok(SqlTypeName::TINYINT),
//             DataType::Int16 => Ok(SqlTypeName::SMALLINT),
//             DataType::Int32 => Ok(SqlTypeName::INTEGER),
//             DataType::Int64 => Ok(SqlTypeName::BIGINT),
//             DataType::UInt8 => Ok(SqlTypeName::TINYINT),
//             DataType::UInt16 => Ok(SqlTypeName::SMALLINT),
//             DataType::UInt32 => Ok(SqlTypeName::INTEGER),
//             DataType::UInt64 => Ok(SqlTypeName::BIGINT),
//             DataType::Float16 => Ok(SqlTypeName::REAL),
//             DataType::Float32 => Ok(SqlTypeName::FLOAT),
//             DataType::Float64 => Ok(SqlTypeName::DOUBLE),
//             DataType::Time32(_) | DataType::Time64(_) => Ok(SqlTypeName::TIME),
//             DataType::Timestamp(_unit, tz) => match tz {
//                 Some(_) => Ok(SqlTypeName::TIMESTAMP_WITH_LOCAL_TIME_ZONE),
//                 None => Ok(SqlTypeName::TIMESTAMP),
//             },
//             DataType::Date32 => Ok(SqlTypeName::DATE),
//             DataType::Date64 => Ok(SqlTypeName::DATE),
//             DataType::Interval(unit) => match unit {
//                 IntervalUnit::DayTime => Ok(SqlTypeName::INTERVAL_DAY),
//                 IntervalUnit::YearMonth => Ok(SqlTypeName::INTERVAL_YEAR_MONTH),
//                 IntervalUnit::MonthDayNano => Ok(SqlTypeName::INTERVAL_MONTH),
//             },
//             DataType::Binary => Ok(SqlTypeName::BINARY),
//             DataType::FixedSizeBinary(_size) => Ok(SqlTypeName::VARBINARY),
//             DataType::Utf8 => Ok(SqlTypeName::CHAR),
//             DataType::LargeUtf8 => Ok(SqlTypeName::VARCHAR),
//             DataType::Struct(_fields) => Ok(SqlTypeName::STRUCTURED),
//             DataType::Decimal128(_precision, _scale) => Ok(SqlTypeName::DECIMAL),
//             DataType::Decimal256(_precision, _scale) => Ok(SqlTypeName::DECIMAL),
//             DataType::Map(_field, _bool) => Ok(SqlTypeName::MAP),
//             _ => Err(DaskPlannerError::Internal(format!(
//                 "Cannot determine Dask SQL type for Arrow type '{:?}'",
//                 arrow_type
//             ))),
//         }
//     }
// }

// #[pymethods]
// impl SqlTypeName {
//     #[pyo3(name = "fromString")]
//     #[staticmethod]
//     pub fn py_from_string(input_type: &str) -> PyResult<Self> {
//         SqlTypeName::from_string(input_type).map_err(|e| e.into())
//     }
// }

// impl SqlTypeName {
//     pub fn from_string(input_type: &str) -> Result<Self, DaskPlannerError> {
//         match input_type.to_uppercase().as_ref() {
//             "ANY" => Ok(SqlTypeName::ANY),
//             "ARRAY" => Ok(SqlTypeName::ARRAY),
//             "NULL" => Ok(SqlTypeName::NULL),
//             "BOOLEAN" => Ok(SqlTypeName::BOOLEAN),
//             "COLUMN_LIST" => Ok(SqlTypeName::COLUMN_LIST),
//             "DISTINCT" => Ok(SqlTypeName::DISTINCT),
//             "CURSOR" => Ok(SqlTypeName::CURSOR),
//             "TINYINT" => Ok(SqlTypeName::TINYINT),
//             "SMALLINT" => Ok(SqlTypeName::SMALLINT),
//             "INT" => Ok(SqlTypeName::INTEGER),
//             "INTEGER" => Ok(SqlTypeName::INTEGER),
//             "BIGINT" => Ok(SqlTypeName::BIGINT),
//             "REAL" => Ok(SqlTypeName::REAL),
//             "FLOAT" => Ok(SqlTypeName::FLOAT),
//             "GEOMETRY" => Ok(SqlTypeName::GEOMETRY),
//             "DOUBLE" => Ok(SqlTypeName::DOUBLE),
//             "TIME" => Ok(SqlTypeName::TIME),
//             "TIME_WITH_LOCAL_TIME_ZONE" => Ok(SqlTypeName::TIME_WITH_LOCAL_TIME_ZONE),
//             "TIMESTAMP" => Ok(SqlTypeName::TIMESTAMP),
//             "TIMESTAMP_WITH_LOCAL_TIME_ZONE" => Ok(SqlTypeName::TIMESTAMP_WITH_LOCAL_TIME_ZONE),
//             "DATE" => Ok(SqlTypeName::DATE),
//             "INTERVAL" => Ok(SqlTypeName::INTERVAL),
//             "INTERVAL_DAY" => Ok(SqlTypeName::INTERVAL_DAY),
//             "INTERVAL_DAY_HOUR" => Ok(SqlTypeName::INTERVAL_DAY_HOUR),
//             "INTERVAL_DAY_MINUTE" => Ok(SqlTypeName::INTERVAL_DAY_MINUTE),
//             "INTERVAL_DAY_SECOND" => Ok(SqlTypeName::INTERVAL_DAY_SECOND),
//             "INTERVAL_HOUR" => Ok(SqlTypeName::INTERVAL_HOUR),
//             "INTERVAL_HOUR_MINUTE" => Ok(SqlTypeName::INTERVAL_HOUR_MINUTE),
//             "INTERVAL_HOUR_SECOND" => Ok(SqlTypeName::INTERVAL_HOUR_SECOND),
//             "INTERVAL_MINUTE" => Ok(SqlTypeName::INTERVAL_MINUTE),
//             "INTERVAL_MINUTE_SECOND" => Ok(SqlTypeName::INTERVAL_MINUTE_SECOND),
//             "INTERVAL_MONTH" => Ok(SqlTypeName::INTERVAL_MONTH),
//             "INTERVAL_SECOND" => Ok(SqlTypeName::INTERVAL_SECOND),
//             "INTERVAL_YEAR" => Ok(SqlTypeName::INTERVAL_YEAR),
//             "INTERVAL_YEAR_MONTH" => Ok(SqlTypeName::INTERVAL_YEAR_MONTH),
//             "MAP" => Ok(SqlTypeName::MAP),
//             "MULTISET" => Ok(SqlTypeName::MULTISET),
//             "OTHER" => Ok(SqlTypeName::OTHER),
//             "ROW" => Ok(SqlTypeName::ROW),
//             "SARG" => Ok(SqlTypeName::SARG),
//             "BINARY" => Ok(SqlTypeName::BINARY),
//             "VARBINARY" => Ok(SqlTypeName::VARBINARY),
//             "CHAR" => Ok(SqlTypeName::CHAR),
//             "VARCHAR" | "STRING" => Ok(SqlTypeName::VARCHAR),
//             "STRUCTURED" => Ok(SqlTypeName::STRUCTURED),
//             "SYMBOL" => Ok(SqlTypeName::SYMBOL),
//             "DECIMAL" => Ok(SqlTypeName::DECIMAL),
//             "DYNAMIC_STAT" => Ok(SqlTypeName::DYNAMIC_STAR),
//             "UNKNOWN" => Ok(SqlTypeName::UNKNOWN),
//             _ => {
//                 // complex data type name so use the sqlparser
//                 let dialect = DaskDialect {};
//                 let mut tokenizer = Tokenizer::new(&dialect, input_type);
//                 let tokens = tokenizer.tokenize().map_err(DaskPlannerError::from)?;
//                 let mut parser = Parser::new(tokens, &dialect);
//                 match parser.parse_data_type().map_err(DaskPlannerError::from)? {
//                     SQLType::Decimal(_) => Ok(SqlTypeName::DECIMAL),
//                     SQLType::Binary(_) => Ok(SqlTypeName::BINARY),
//                     SQLType::Varbinary(_) => Ok(SqlTypeName::VARBINARY),
//                     SQLType::Varchar(_) | SQLType::Nvarchar(_) => Ok(SqlTypeName::VARCHAR),
//                     SQLType::Char(_) => Ok(SqlTypeName::CHAR),
//                     _ => Err(DaskPlannerError::Internal(format!(
//                         "Cannot determine Dask SQL type for '{}'",
//                         input_type
//                     ))),
//                 }
//             }
//         }
//     }
// }

// #[cfg(test)]
// mod test {
//     use crate::sql::types::SqlTypeName;

//     #[test]
//     fn invalid_type_name() {
//         assert_eq!(
//             "Internal Error: Cannot determine Dask SQL type for 'bob'",
//             SqlTypeName::from_string("bob")
//                 .expect_err("invalid type name")
//                 .to_string()
//         );
//     }

//     #[test]
//     fn string() {
//         assert_expected("VARCHAR", "string");
//     }

//     #[test]
//     fn varchar_n() {
//         assert_expected("VARCHAR", "VARCHAR(10)");
//     }

//     #[test]
//     fn decimal_p_s() {
//         assert_expected("DECIMAL", "DECIMAL(10, 2)");
//     }

//     fn assert_expected(expected: &str, input: &str) {
//         assert_eq!(
//             expected,
//             &format!("{:?}", SqlTypeName::from_string(input).unwrap())
//         );
//     }
// }
