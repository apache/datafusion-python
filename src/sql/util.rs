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

use datafusion::common::{exec_err, plan_datafusion_err, DataFusionError};
use datafusion::logical_expr::sqlparser::dialect::dialect_from_str;
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

fn tokens_from_replacements(
    placeholder: &str,
    replacements: &HashMap<String, Vec<Token>>,
) -> Option<Vec<Token>> {
    if let Some(pattern) = placeholder.strip_prefix("$") {
        replacements.get(pattern).cloned()
    } else {
        None
    }
}

fn get_tokens_for_string_replacement(
    dialect: &dyn Dialect,
    replacements: HashMap<String, String>,
) -> Result<HashMap<String, Vec<Token>>, DataFusionError> {
    replacements
        .into_iter()
        .map(|(name, value)| {
            let tokens = Tokenizer::new(dialect, &value)
                .tokenize()
                .map_err(|err| DataFusionError::External(err.into()))?;
            Ok((name, tokens))
        })
        .collect()
}

pub(crate) fn replace_placeholders_with_strings(
    query: &str,
    dialect: &str,
    replacements: HashMap<String, String>,
) -> Result<String, DataFusionError> {
    let dialect = dialect_from_str(dialect)
        .ok_or_else(|| plan_datafusion_err!("Unsupported SQL dialect: {dialect}."))?;

    let replacements = get_tokens_for_string_replacement(dialect.as_ref(), replacements)?;

    let tokens = Tokenizer::new(dialect.as_ref(), query)
        .tokenize()
        .map_err(|err| DataFusionError::External(err.into()))?;

    let replaced_tokens = tokens
        .into_iter()
        .flat_map(|token| {
            if let Token::Placeholder(placeholder) = &token {
                tokens_from_replacements(placeholder, &replacements).unwrap_or(vec![token])
            } else {
                vec![token]
            }
        })
        .collect::<Vec<Token>>();

    let statement = Parser::new(dialect.as_ref())
        .with_tokens(replaced_tokens)
        .parse_statements()
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

    if statement.len() != 1 {
        return exec_err!("placeholder replacement should return exactly one statement");
    }

    Ok(statement[0].to_string())
}
