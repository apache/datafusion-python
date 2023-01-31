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

use std::collections::HashMap;
use std::fmt;

use datafusion_common::{DFField, DFSchema};
use pyo3::prelude::*;

use super::DataTypeMap;

const PRECISION_NOT_SPECIFIED: i32 = i32::MIN;
const SCALE_NOT_SPECIFIED: i32 = -1;

/// RelDataTypeField represents the definition of a field in a structured RelDataType.
#[pyclass(name = "RelDataTypeField", module = "dask_planner", subclass)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelDataTypeField {
    qualifier: Option<String>,
    name: String,
    data_type: DataTypeMap,
    index: usize,
}

// // Functions that should not be presented to Python are placed here
// impl RelDataTypeField {
//     pub fn from(field: &DFField, schema: &DFSchema) -> Result<RelDataTypeField> {
//         let qualifier: Option<&str> = match field.qualifier() {
//             Some(qualifier) => Some(qualifier),
//             None => None,
//         };
//         Ok(RelDataTypeField {
//             qualifier: qualifier.map(|qualifier| qualifier.to_string()),
//             name: field.name().clone(),
//             data_type: DaskTypeMap {
//                 sql_type: SqlTypeName::from_arrow(field.data_type())?,
//                 data_type: field.data_type().clone().into(),
//             },
//             index: schema.index_of_column_by_name(qualifier, field.name())?,
//         })
//     }
// }

// #[pymethods]
// impl RelDataTypeField {
//     #[new]
//     pub fn new(name: &str, type_map: DaskTypeMap, index: usize) -> Self {
//         Self {
//             qualifier: None,
//             name: name.to_owned(),
//             data_type: type_map,
//             index,
//         }
//     }

//     #[pyo3(name = "getQualifier")]
//     pub fn qualifier(&self) -> Option<String> {
//         self.qualifier.clone()
//     }

//     #[pyo3(name = "getName")]
//     pub fn name(&self) -> &str {
//         &self.name
//     }

//     #[pyo3(name = "getQualifiedName")]
//     pub fn qualified_name(&self) -> String {
//         match &self.qualifier() {
//             Some(qualifier) => format!("{}.{}", &qualifier, self.name()),
//             None => self.name().to_string(),
//         }
//     }

//     #[pyo3(name = "getIndex")]
//     pub fn index(&self) -> usize {
//         self.index
//     }

//     #[pyo3(name = "getType")]
//     pub fn data_type(&self) -> DaskTypeMap {
//         self.data_type.clone()
//     }

//     /// Since this logic is being ported from Java getKey is synonymous with getName.
//     /// Alas it is used in certain places so it is implemented here to allow other
//     /// places in the code base to not have to change.
//     #[pyo3(name = "getKey")]
//     pub fn get_key(&self) -> &str {
//         self.name()
//     }

//     /// Since this logic is being ported from Java getValue is synonymous with getType.
//     /// Alas it is used in certain places so it is implemented here to allow other
//     /// places in the code base to not have to change.
//     #[pyo3(name = "getValue")]
//     pub fn get_value(&self) -> DaskTypeMap {
//         self.data_type()
//     }

//     #[pyo3(name = "setValue")]
//     pub fn set_value(&mut self, data_type: DaskTypeMap) {
//         self.data_type = data_type
//     }

//     // TODO: Uncomment after implementing in RelDataType
//     // #[pyo3(name = "isDynamicStar")]
//     // pub fn is_dynamic_star(&self) -> bool {
//     //     self.data_type.getSqlTypeName() == SqlTypeName.DYNAMIC_STAR
//     // }
// }

// impl fmt::Display for RelDataTypeField {
//     fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
//         fmt.write_str("Field: ")?;
//         fmt.write_str(&self.name)?;
//         fmt.write_str(" - Index: ")?;
//         fmt.write_str(&self.index.to_string())?;
//         // TODO: Uncomment this after implementing the Display trait in RelDataType
//         // fmt.write_str(" - DataType: ")?;
//         // fmt.write_str(self.data_type.to_string())?;
//         Ok(())
//     }
// }

// /// RelDataType represents the type of a scalar expression or entire row returned from a relational expression.
// #[pyclass(name = "RelDataType", module = "dask_planner", subclass)]
// #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
// pub struct RelDataType {
//     nullable: bool,
//     field_list: Vec<RelDataTypeField>,
// }

// /// RelDataType represents the type of a scalar expression or entire row returned from a relational expression.
// #[pymethods]
// impl RelDataType {
//     #[new]
//     pub fn new(nullable: bool, fields: Vec<RelDataTypeField>) -> Self {
//         Self {
//             nullable,
//             field_list: fields,
//         }
//     }

//     /// Looks up a field by name.
//     ///
//     /// # Arguments
//     ///
//     /// * `field_name` - A String containing the name of the field to find
//     /// * `case_sensitive` - True if column name matching should be case sensitive and false otherwise
//     #[pyo3(name = "getField")]
//     pub fn field(&self, field_name: &str, case_sensitive: bool) -> PyResult<RelDataTypeField> {
//         let field_map: HashMap<String, RelDataTypeField> = self.field_map();
//         if case_sensitive && !field_map.is_empty() {
//             Ok(field_map.get(field_name).unwrap().clone())
//         } else {
//             for field in &self.field_list {
//                 if (case_sensitive && field.name().eq(field_name))
//                     || (!case_sensitive && field.name().eq_ignore_ascii_case(field_name))
//                 {
//                     return Ok(field.clone());
//                 }
//             }

//             // TODO: Throw a proper error here
//             Err(py_runtime_err(format!(
//                 "Unable to find RelDataTypeField with name {:?} in the RelDataType field_list",
//                 field_name,
//             )))
//         }
//     }

//     /// Returns a map from field names to fields.
//     ///
//     /// # Notes
//     ///
//     /// * If several fields have the same name, the map contains the first.
//     #[pyo3(name = "getFieldMap")]
//     pub fn field_map(&self) -> HashMap<String, RelDataTypeField> {
//         let mut fields: HashMap<String, RelDataTypeField> = HashMap::new();
//         for field in &self.field_list {
//             fields.insert(String::from(field.name()), field.clone());
//         }
//         fields
//     }

//     /// Gets the fields in a struct type. The field count is equal to the size of the returned list.
//     #[pyo3(name = "getFieldList")]
//     pub fn field_list(&self) -> Vec<RelDataTypeField> {
//         self.field_list.clone()
//     }

//     /// Returns the names of all of the columns in a given DaskTable
//     #[pyo3(name = "getFieldNames")]
//     pub fn field_names(&self) -> Vec<String> {
//         let mut field_names: Vec<String> = Vec::new();
//         for field in &self.field_list {
//             field_names.push(field.qualified_name());
//         }
//         field_names
//     }

//     /// Returns the number of fields in a struct type.
//     #[pyo3(name = "getFieldCount")]
//     pub fn field_count(&self) -> usize {
//         self.field_list.len()
//     }

//     #[pyo3(name = "isStruct")]
//     pub fn is_struct(&self) -> bool {
//         !self.field_list.is_empty()
//     }

//     /// Queries whether this type allows null values.
//     #[pyo3(name = "isNullable")]
//     pub fn is_nullable(&self) -> bool {
//         self.nullable
//     }

//     #[pyo3(name = "getPrecision")]
//     pub fn precision(&self) -> i32 {
//         PRECISION_NOT_SPECIFIED
//     }

//     #[pyo3(name = "getScale")]
//     pub fn scale(&self) -> i32 {
//         SCALE_NOT_SPECIFIED
//     }
// }
