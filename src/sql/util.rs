use datafusion::common::{internal_err, plan_datafusion_err, DataFusionError};
use datafusion::logical_expr::sqlparser::dialect::dialect_from_str;
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use std::collections::HashMap;

fn value_from_replacements(
    placeholder: &str,
    replacements: &HashMap<String, String>,
) -> Option<Token> {
    if let Some(pattern) = placeholder.strip_prefix("$") {
        replacements.get(pattern).map(|replacement| {
            Token::Word(Word {
                value: replacement.to_owned(),
                quote_style: None,
                keyword: Keyword::NoKeyword,
            })
        })
    } else {
        None
    }
}

fn table_names_are_valid(dialect: &dyn Dialect, replacements: &HashMap<String, String>) -> bool {
    for name in replacements.values() {
        let tokens = Tokenizer::new(dialect, name).tokenize().unwrap();
        if tokens.len() != 1 {
            // We should get exactly one token for our temporary table name
            return false;
        }

        if let Token::Word(word) = &tokens[0] {
            // Generated table names should be not quoted or have keywords
            if word.quote_style.is_some() || word.keyword != Keyword::NoKeyword {
                return false;
            }
        } else {
            // We should always parse table names to a Word
            return false;
        }
    }

    true
}

pub(crate) fn replace_placeholders_with_table_names(
    query: &str,
    dialect: &str,
    replacements: HashMap<String, String>,
) -> Result<String, DataFusionError> {
    let dialect = dialect_from_str(dialect).ok_or_else(|| {
        plan_datafusion_err!(
            "Unsupported SQL dialect: {dialect}. Available dialects: \
                     Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, \
                     MsSQL, ClickHouse, BigQuery, Ansi, DuckDB, Databricks."
        )
    })?;

    if !table_names_are_valid(dialect.as_ref(), &replacements) {
        return internal_err!("Invalid generated table name when replacing placeholders");
    }
    let tokens = Tokenizer::new(dialect.as_ref(), query).tokenize().unwrap();

    let replaced_tokens = tokens
        .into_iter()
        .map(|token| {
            if let Token::Word(word) = &token {
                let Word {
                    value,
                    quote_style: _,
                    keyword: _,
                } = word;

                value_from_replacements(value, &replacements).unwrap_or(token)
            } else if let Token::Placeholder(placeholder) = &token {
                value_from_replacements(placeholder, &replacements).unwrap_or(token)
            } else {
                token
            }
        })
        .collect::<Vec<Token>>();

    Ok(Parser::new(dialect.as_ref())
        .with_tokens(replaced_tokens)
        .parse_statements()
        .map_err(|err| DataFusionError::External(Box::new(err)))?
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join(" "))
}
