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

use pyo3::{prelude::*, wrap_pyfunction};

use datafusion_common::Column;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::expr::{Sort, WindowFunction};
use datafusion_expr::window_function::find_df_window_func;
use datafusion_expr::{aggregate_function, lit, BuiltinScalarFunction, Expr, WindowFrame};

use crate::errors::DataFusionError;
use crate::expr::PyExpr;

#[pyfunction]
fn in_list(expr: PyExpr, value: Vec<PyExpr>, negated: bool) -> PyExpr {
    datafusion_expr::in_list(
        expr.expr,
        value.into_iter().map(|x| x.expr).collect::<Vec<_>>(),
        negated,
    )
    .into()
}

/// Computes a binary hash of the given data. type is the algorithm to use.
/// Standard algorithms are md5, sha224, sha256, sha384, sha512, blake2s, blake2b, and blake3.
// #[pyfunction(value, method)]
#[pyfunction]
#[pyo3(signature = (value, method))]
fn digest(value: PyExpr, method: PyExpr) -> PyExpr {
    PyExpr {
        expr: datafusion_expr::digest(value.expr, method.expr),
    }
}

/// Concatenates the text representations of all the arguments.
/// NULL arguments are ignored.
#[pyfunction]
#[pyo3(signature = (*args))]
fn concat(args: Vec<PyExpr>) -> PyResult<PyExpr> {
    let args = args.into_iter().map(|e| e.expr).collect::<Vec<_>>();
    Ok(datafusion_expr::concat(&args).into())
}

/// Concatenates all but the first argument, with separators.
/// The first argument is used as the separator string, and should not be NULL.
/// Other NULL arguments are ignored.
#[pyfunction]
#[pyo3(signature = (sep, *args))]
fn concat_ws(sep: String, args: Vec<PyExpr>) -> PyResult<PyExpr> {
    let args = args.into_iter().map(|e| e.expr).collect::<Vec<_>>();
    Ok(datafusion_expr::concat_ws(lit(sep), args).into())
}

/// Creates a new Sort Expr
#[pyfunction]
fn order_by(expr: PyExpr, asc: Option<bool>, nulls_first: Option<bool>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        expr: datafusion_expr::Expr::Sort(Sort {
            expr: Box::new(expr.expr),
            asc: asc.unwrap_or(true),
            nulls_first: nulls_first.unwrap_or(true),
        }),
    })
}

/// Creates a new Alias Expr
#[pyfunction]
fn alias(expr: PyExpr, name: &str) -> PyResult<PyExpr> {
    Ok(PyExpr {
        expr: datafusion_expr::Expr::Alias(Box::new(expr.expr), String::from(name)),
    })
}

/// Create a column reference Expr
#[pyfunction]
fn col(name: &str) -> PyResult<PyExpr> {
    Ok(PyExpr {
        expr: datafusion_expr::Expr::Column(Column {
            relation: None,
            name: name.to_string(),
        }),
    })
}

/// Create a COUNT(1) aggregate expression
#[pyfunction]
fn count_star() -> PyResult<PyExpr> {
    Ok(PyExpr {
        expr: Expr::AggregateFunction(AggregateFunction {
            fun: aggregate_function::AggregateFunction::Count,
            args: vec![lit(1)],
            distinct: false,
            filter: None,
        }),
    })
}

/// Creates a new Window function expression
#[pyfunction]
fn window(
    name: &str,
    args: Vec<PyExpr>,
    partition_by: Option<Vec<PyExpr>>,
    order_by: Option<Vec<PyExpr>>,
) -> PyResult<PyExpr> {
    let fun = find_df_window_func(name);
    if fun.is_none() {
        return Err(DataFusionError::Common("window function not found".to_string()).into());
    }
    let fun = fun.unwrap();
    let window_frame = WindowFrame::new(order_by.is_some());
    Ok(PyExpr {
        expr: datafusion_expr::Expr::WindowFunction(WindowFunction {
            fun,
            args: args.into_iter().map(|x| x.expr).collect::<Vec<_>>(),
            partition_by: partition_by
                .unwrap_or_default()
                .into_iter()
                .map(|x| x.expr)
                .collect::<Vec<_>>(),
            order_by: order_by
                .unwrap_or_default()
                .into_iter()
                .map(|x| x.expr)
                .collect::<Vec<_>>(),
            window_frame,
        }),
    })
}

macro_rules! scalar_function {
    ($NAME: ident, $FUNC: ident) => {
        scalar_function!($NAME, $FUNC, stringify!($NAME));
    };

    ($NAME: ident, $FUNC: ident, $DOC: expr) => {
        #[doc = $DOC]
        #[pyfunction]
        #[pyo3(signature = (*args))]
        fn $NAME(args: Vec<PyExpr>) -> PyExpr {
            let expr = datafusion_expr::Expr::ScalarFunction {
                fun: BuiltinScalarFunction::$FUNC,
                args: args.into_iter().map(|e| e.into()).collect(),
            };
            expr.into()
        }
    };
}

macro_rules! aggregate_function {
    ($NAME: ident, $FUNC: ident) => {
        aggregate_function!($NAME, $FUNC, stringify!($NAME));
    };
    ($NAME: ident, $FUNC: ident, $DOC: expr) => {
        #[doc = $DOC]
        #[pyfunction]
        #[pyo3(signature = (*args, distinct=false))]
        fn $NAME(args: Vec<PyExpr>, distinct: bool) -> PyExpr {
            let expr = datafusion_expr::Expr::AggregateFunction(AggregateFunction {
                fun: datafusion_expr::aggregate_function::AggregateFunction::$FUNC,
                args: args.into_iter().map(|e| e.into()).collect(),
                distinct,
                filter: None,
            });
            expr.into()
        }
    };
}

scalar_function!(abs, Abs);
scalar_function!(acos, Acos);
scalar_function!(ascii, Ascii, "Returns the numeric code of the first character of the argument. In UTF8 encoding, returns the Unicode code point of the character. In other multibyte encodings, the argument must be an ASCII character.");
scalar_function!(asin, Asin);
scalar_function!(atan, Atan);
scalar_function!(atan2, Atan2);
scalar_function!(
    bit_length,
    BitLength,
    "Returns number of bits in the string (8 times the octet_length)."
);
scalar_function!(btrim, Btrim, "Removes the longest string containing only characters in characters (a space by default) from the start and end of string.");
scalar_function!(ceil, Ceil);
scalar_function!(
    character_length,
    CharacterLength,
    "Returns number of characters in the string."
);
scalar_function!(length, CharacterLength);
scalar_function!(char_length, CharacterLength);
scalar_function!(chr, Chr, "Returns the character with the given code.");
scalar_function!(coalesce, Coalesce);
scalar_function!(cos, Cos);
scalar_function!(exp, Exp);
scalar_function!(floor, Floor);
scalar_function!(initcap, InitCap, "Converts the first letter of each word to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.");
scalar_function!(left, Left, "Returns first n characters in the string, or when n is negative, returns all but last |n| characters.");
scalar_function!(ln, Ln);
scalar_function!(log, Log);
scalar_function!(log10, Log10);
scalar_function!(log2, Log2);
scalar_function!(lower, Lower, "Converts the string to all lower case");
scalar_function!(lpad, Lpad, "Extends the string to length length by prepending the characters fill (a space by default). If the string is already longer than length then it is truncated (on the right).");
scalar_function!(ltrim, Ltrim, "Removes the longest string containing only characters in characters (a space by default) from the start of string.");
scalar_function!(
    md5,
    MD5,
    "Computes the MD5 hash of the argument, with the result written in hexadecimal."
);
scalar_function!(octet_length, OctetLength, "Returns number of bytes in the string. Since this version of the function accepts type character directly, it will not strip trailing spaces.");
scalar_function!(power, Power);
scalar_function!(pow, Power);
scalar_function!(regexp_match, RegexpMatch);
scalar_function!(
    regexp_replace,
    RegexpReplace,
    "Replaces substring(s) matching a POSIX regular expression"
);
scalar_function!(
    repeat,
    Repeat,
    "Repeats string the specified number of times."
);
scalar_function!(
    replace,
    Replace,
    "Replaces all occurrences in string of substring from with substring to."
);
scalar_function!(
    reverse,
    Reverse,
    "Reverses the order of the characters in the string."
);
scalar_function!(right, Right, "Returns last n characters in the string, or when n is negative, returns all but first |n| characters.");
scalar_function!(round, Round);
scalar_function!(rpad, Rpad, "Extends the string to length length by appending the characters fill (a space by default). If the string is already longer than length then it is truncated.");
scalar_function!(rtrim, Rtrim, "Removes the longest string containing only characters in characters (a space by default) from the end of string.");
scalar_function!(sha224, SHA224);
scalar_function!(sha256, SHA256);
scalar_function!(sha384, SHA384);
scalar_function!(sha512, SHA512);
scalar_function!(signum, Signum);
scalar_function!(sin, Sin);
scalar_function!(
    split_part,
    SplitPart,
    "Splits string at occurrences of delimiter and returns the n'th field (counting from one)."
);
scalar_function!(sqrt, Sqrt);
scalar_function!(
    starts_with,
    StartsWith,
    "Returns true if string starts with prefix."
);
scalar_function!(strpos, Strpos, "Returns starting index of specified substring within string, or zero if it's not present. (Same as position(substring in string), but note the reversed argument order.)");
scalar_function!(substr, Substr);
scalar_function!(tan, Tan);
scalar_function!(
    to_hex,
    ToHex,
    "Converts the number to its equivalent hexadecimal representation."
);
scalar_function!(now, Now);
scalar_function!(to_timestamp, ToTimestamp);
scalar_function!(to_timestamp_millis, ToTimestampMillis);
scalar_function!(to_timestamp_micros, ToTimestampMicros);
scalar_function!(to_timestamp_seconds, ToTimestampSeconds);
scalar_function!(current_date, CurrentDate);
scalar_function!(current_time, CurrentTime);
scalar_function!(datepart, DatePart);
scalar_function!(date_part, DatePart);
scalar_function!(date_trunc, DateTrunc);
scalar_function!(datetrunc, DateTrunc);
scalar_function!(date_bin, DateBin);
scalar_function!(translate, Translate, "Replaces each character in string that matches a character in the from set with the corresponding character in the to set. If from is longer than to, occurrences of the extra characters in from are deleted.");
scalar_function!(trim, Trim, "Removes the longest string containing only characters in characters (a space by default) from the start, end, or both ends (BOTH is the default) of string.");
scalar_function!(trunc, Trunc);
scalar_function!(upper, Upper, "Converts the string to all upper case.");
scalar_function!(make_array, MakeArray);
scalar_function!(array, MakeArray);
scalar_function!(nullif, NullIf);
scalar_function!(uuid, Uuid);
scalar_function!(r#struct, Struct); // Use raw identifier since struct is a keyword
scalar_function!(from_unixtime, FromUnixtime);
scalar_function!(arrow_typeof, ArrowTypeof);
scalar_function!(random, Random);

aggregate_function!(approx_distinct, ApproxDistinct);
aggregate_function!(approx_median, ApproxMedian);
aggregate_function!(approx_percentile_cont, ApproxPercentileCont);
aggregate_function!(
    approx_percentile_cont_with_weight,
    ApproxPercentileContWithWeight
);
aggregate_function!(array_agg, ArrayAgg);
aggregate_function!(avg, Avg);
aggregate_function!(corr, Correlation);
aggregate_function!(count, Count);
aggregate_function!(covar, Covariance);
aggregate_function!(covar_pop, CovariancePop);
aggregate_function!(covar_samp, Covariance);
aggregate_function!(grouping, Grouping);
aggregate_function!(max, Max);
aggregate_function!(mean, Avg);
aggregate_function!(median, Median);
aggregate_function!(min, Min);
aggregate_function!(sum, Sum);
aggregate_function!(stddev, Stddev);
aggregate_function!(stddev_pop, StddevPop);
aggregate_function!(stddev_samp, Stddev);
aggregate_function!(var, Variance);
aggregate_function!(var_pop, VariancePop);
aggregate_function!(var_samp, Variance);

pub(crate) fn init_module(m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(abs))?;
    m.add_wrapped(wrap_pyfunction!(acos))?;
    m.add_wrapped(wrap_pyfunction!(approx_distinct))?;
    m.add_wrapped(wrap_pyfunction!(alias))?;
    m.add_wrapped(wrap_pyfunction!(approx_median))?;
    m.add_wrapped(wrap_pyfunction!(approx_percentile_cont))?;
    m.add_wrapped(wrap_pyfunction!(approx_percentile_cont_with_weight))?;
    m.add_wrapped(wrap_pyfunction!(array))?;
    m.add_wrapped(wrap_pyfunction!(array_agg))?;
    m.add_wrapped(wrap_pyfunction!(arrow_typeof))?;
    m.add_wrapped(wrap_pyfunction!(ascii))?;
    m.add_wrapped(wrap_pyfunction!(asin))?;
    m.add_wrapped(wrap_pyfunction!(atan))?;
    m.add_wrapped(wrap_pyfunction!(atan2))?;
    m.add_wrapped(wrap_pyfunction!(avg))?;
    m.add_wrapped(wrap_pyfunction!(bit_length))?;
    m.add_wrapped(wrap_pyfunction!(btrim))?;
    m.add_wrapped(wrap_pyfunction!(ceil))?;
    m.add_wrapped(wrap_pyfunction!(character_length))?;
    m.add_wrapped(wrap_pyfunction!(chr))?;
    m.add_wrapped(wrap_pyfunction!(char_length))?;
    m.add_wrapped(wrap_pyfunction!(coalesce))?;
    m.add_wrapped(wrap_pyfunction!(col))?;
    m.add_wrapped(wrap_pyfunction!(concat_ws))?;
    m.add_wrapped(wrap_pyfunction!(concat))?;
    m.add_wrapped(wrap_pyfunction!(corr))?;
    m.add_wrapped(wrap_pyfunction!(cos))?;
    m.add_wrapped(wrap_pyfunction!(count))?;
    m.add_wrapped(wrap_pyfunction!(count_star))?;
    m.add_wrapped(wrap_pyfunction!(covar))?;
    m.add_wrapped(wrap_pyfunction!(covar_pop))?;
    m.add_wrapped(wrap_pyfunction!(covar_samp))?;
    m.add_wrapped(wrap_pyfunction!(current_date))?;
    m.add_wrapped(wrap_pyfunction!(current_time))?;
    m.add_wrapped(wrap_pyfunction!(date_bin))?;
    m.add_wrapped(wrap_pyfunction!(datepart))?;
    m.add_wrapped(wrap_pyfunction!(date_part))?;
    m.add_wrapped(wrap_pyfunction!(datetrunc))?;
    m.add_wrapped(wrap_pyfunction!(date_trunc))?;
    m.add_wrapped(wrap_pyfunction!(digest))?;
    m.add_wrapped(wrap_pyfunction!(exp))?;
    m.add_wrapped(wrap_pyfunction!(floor))?;
    m.add_wrapped(wrap_pyfunction!(from_unixtime))?;
    m.add_wrapped(wrap_pyfunction!(grouping))?;
    m.add_wrapped(wrap_pyfunction!(in_list))?;
    m.add_wrapped(wrap_pyfunction!(initcap))?;
    m.add_wrapped(wrap_pyfunction!(left))?;
    m.add_wrapped(wrap_pyfunction!(length))?;
    m.add_wrapped(wrap_pyfunction!(ln))?;
    m.add_wrapped(wrap_pyfunction!(log))?;
    m.add_wrapped(wrap_pyfunction!(log10))?;
    m.add_wrapped(wrap_pyfunction!(log2))?;
    m.add_wrapped(wrap_pyfunction!(lower))?;
    m.add_wrapped(wrap_pyfunction!(lpad))?;
    m.add_wrapped(wrap_pyfunction!(ltrim))?;
    m.add_wrapped(wrap_pyfunction!(max))?;
    m.add_wrapped(wrap_pyfunction!(make_array))?;
    m.add_wrapped(wrap_pyfunction!(md5))?;
    m.add_wrapped(wrap_pyfunction!(mean))?;
    m.add_wrapped(wrap_pyfunction!(median))?;
    m.add_wrapped(wrap_pyfunction!(min))?;
    m.add_wrapped(wrap_pyfunction!(now))?;
    m.add_wrapped(wrap_pyfunction!(nullif))?;
    m.add_wrapped(wrap_pyfunction!(octet_length))?;
    m.add_wrapped(wrap_pyfunction!(order_by))?;
    m.add_wrapped(wrap_pyfunction!(power))?;
    m.add_wrapped(wrap_pyfunction!(pow))?;
    m.add_wrapped(wrap_pyfunction!(random))?;
    m.add_wrapped(wrap_pyfunction!(regexp_match))?;
    m.add_wrapped(wrap_pyfunction!(regexp_replace))?;
    m.add_wrapped(wrap_pyfunction!(repeat))?;
    m.add_wrapped(wrap_pyfunction!(replace))?;
    m.add_wrapped(wrap_pyfunction!(reverse))?;
    m.add_wrapped(wrap_pyfunction!(right))?;
    m.add_wrapped(wrap_pyfunction!(round))?;
    m.add_wrapped(wrap_pyfunction!(rpad))?;
    m.add_wrapped(wrap_pyfunction!(rtrim))?;
    m.add_wrapped(wrap_pyfunction!(sha224))?;
    m.add_wrapped(wrap_pyfunction!(sha256))?;
    m.add_wrapped(wrap_pyfunction!(sha384))?;
    m.add_wrapped(wrap_pyfunction!(sha512))?;
    m.add_wrapped(wrap_pyfunction!(signum))?;
    m.add_wrapped(wrap_pyfunction!(sin))?;
    m.add_wrapped(wrap_pyfunction!(split_part))?;
    m.add_wrapped(wrap_pyfunction!(sqrt))?;
    m.add_wrapped(wrap_pyfunction!(starts_with))?;
    m.add_wrapped(wrap_pyfunction!(stddev))?;
    m.add_wrapped(wrap_pyfunction!(stddev_pop))?;
    m.add_wrapped(wrap_pyfunction!(stddev_samp))?;
    m.add_wrapped(wrap_pyfunction!(strpos))?;
    m.add_wrapped(wrap_pyfunction!(r#struct))?; // Use raw identifier since struct is a keyword
    m.add_wrapped(wrap_pyfunction!(substr))?;
    m.add_wrapped(wrap_pyfunction!(sum))?;
    m.add_wrapped(wrap_pyfunction!(tan))?;
    m.add_wrapped(wrap_pyfunction!(to_hex))?;
    m.add_wrapped(wrap_pyfunction!(to_timestamp))?;
    m.add_wrapped(wrap_pyfunction!(to_timestamp_millis))?;
    m.add_wrapped(wrap_pyfunction!(to_timestamp_micros))?;
    m.add_wrapped(wrap_pyfunction!(to_timestamp_seconds))?;
    m.add_wrapped(wrap_pyfunction!(translate))?;
    m.add_wrapped(wrap_pyfunction!(trim))?;
    m.add_wrapped(wrap_pyfunction!(trunc))?;
    m.add_wrapped(wrap_pyfunction!(upper))?;
    m.add_wrapped(wrap_pyfunction!(self::uuid))?; // Use self to avoid name collision
    m.add_wrapped(wrap_pyfunction!(var))?;
    m.add_wrapped(wrap_pyfunction!(var_pop))?;
    m.add_wrapped(wrap_pyfunction!(var_samp))?;
    m.add_wrapped(wrap_pyfunction!(window))?;
    Ok(())
}
