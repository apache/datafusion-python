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

//! PyO3 wrappers for the [`datafusion-spark`] crate.
//!
//! Exposes Spark-compatible scalar and aggregate function builders for use
//! from Python under `datafusion.functions.spark`.

use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion_spark::{expr_fn, function as udf};
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::common::data_type::NullTreatment;
use crate::errors::PyDataFusionResult;
use crate::expr::PyExpr;
use crate::expr::sort_expr::PySortExpr;
use crate::functions::add_builder_fns_to_aggregate;

/// Generates a [pyo3] wrapper for [datafusion_spark::expr_fn].
///
/// These functions have explicit named arguments and mirror the upstream
/// `expr_fn::$FUNC` signature.
macro_rules! spark_expr_fn {
    ($FUNC:ident) => {
        spark_expr_fn!($FUNC,);
    };
    ($FUNC:ident, $($arg:ident)*) => {
        #[pyfunction]
        fn $FUNC($($arg: PyExpr),*) -> PyExpr {
            expr_fn::$FUNC($($arg.into()),*).into()
        }
    };
}

/// Generates a variadic [pyo3] wrapper that calls the [`ScalarUDF`] factory
/// directly. Required for functions whose upstream `expr_fn` wrapper accepts
/// a single `Expr` instead of `Vec<Expr>` (an upstream `export_functions!`
/// macro-arm quirk), so we bypass it to get true Python `*args` semantics.
macro_rules! spark_udf_vec {
    ($PY_NAME:ident, $UDF_PATH:path) => {
        #[pyfunction]
        #[pyo3(signature = (*args))]
        fn $PY_NAME(args: Vec<PyExpr>) -> PyExpr {
            let udf = $UDF_PATH();
            let args: Vec<Expr> = args.into_iter().map(Into::into).collect();
            Expr::ScalarFunction(ScalarFunction::new_udf(udf, args)).into()
        }
    };
}

/// Generates a [pyo3] wrapper for Spark aggregate functions. Mirrors
/// [`crate::functions::aggregate_function`] but points at
/// [`datafusion_spark::expr_fn`].
macro_rules! spark_aggregate {
    ($NAME:ident) => {
        spark_aggregate!($NAME, expr);
    };
    ($NAME:ident, $($arg:ident)*) => {
        #[pyfunction]
        #[pyo3(signature = ($($arg),*, distinct=None, filter=None, order_by=None, null_treatment=None))]
        fn $NAME(
            $($arg: PyExpr),*,
            distinct: Option<bool>,
            filter: Option<PyExpr>,
            order_by: Option<Vec<PySortExpr>>,
            null_treatment: Option<NullTreatment>,
        ) -> PyDataFusionResult<PyExpr> {
            let agg_fn = expr_fn::$NAME($($arg.into()),*);
            add_builder_fns_to_aggregate(agg_fn, distinct, filter, order_by, null_treatment)
        }
    };
}

// ---------------------------------------------------------------------------
// Aggregate functions
// ---------------------------------------------------------------------------

spark_aggregate!(avg, arg1);
spark_aggregate!(try_sum, arg1);
spark_aggregate!(collect_list, arg1);
spark_aggregate!(collect_set, arg1);

// ---------------------------------------------------------------------------
// Array functions
// ---------------------------------------------------------------------------

// Upstream factory is `spark_array_contains`; expose under the Spark SQL
// name `array_contains` on the Python side.
#[pyfunction]
fn array_contains(arr: PyExpr, element: PyExpr) -> PyExpr {
    expr_fn::spark_array_contains(arr.into(), element.into()).into()
}
spark_udf_vec!(array, udf::array::array);
spark_expr_fn!(shuffle, arg1);
spark_expr_fn!(array_repeat, element count);
spark_expr_fn!(slice, arr start length);

// ---------------------------------------------------------------------------
// Bitmap functions
// ---------------------------------------------------------------------------

spark_expr_fn!(bitmap_count, arg1);
spark_expr_fn!(bitmap_bit_position, arg1);
spark_expr_fn!(bitmap_bucket_number, arg1);

// ---------------------------------------------------------------------------
// Bitwise functions
// ---------------------------------------------------------------------------

spark_expr_fn!(bit_get, col pos);
spark_expr_fn!(bit_count, col);
spark_expr_fn!(bitwise_not, col);
spark_expr_fn!(shiftleft, value shift);
spark_expr_fn!(shiftright, value shift);
spark_expr_fn!(shiftrightunsigned, value shift);

// ---------------------------------------------------------------------------
// Collection / Conditional / Conversion
// ---------------------------------------------------------------------------

spark_expr_fn!(size, arg1);

// Python keyword `if` → exposed as `if_`. Upstream Rust ident is `r#if`.
#[pyfunction]
fn if_(condition: PyExpr, if_true: PyExpr, if_false: PyExpr) -> PyExpr {
    expr_fn::r#if(condition.into(), if_true.into(), if_false.into()).into()
}

// `spark_cast` is config-injected by the upstream `expr_fn` helper; defaults
// applied automatically there.
spark_expr_fn!(spark_cast, arg1 arg2);

// ---------------------------------------------------------------------------
// Datetime functions
// ---------------------------------------------------------------------------

spark_expr_fn!(add_months, start_date num_months);
spark_expr_fn!(date_add, start_date days);
spark_expr_fn!(date_sub, start_date days);
spark_expr_fn!(hour, arg1);
spark_expr_fn!(minute, arg1);
spark_expr_fn!(second, arg1);
spark_expr_fn!(last_day, arg1);
spark_expr_fn!(make_dt_interval, days hours mins secs);
spark_expr_fn!(make_interval, years months weeks days hours mins secs);
spark_expr_fn!(next_day, start_date day_of_week);
spark_expr_fn!(date_diff, end_date start_date);
spark_expr_fn!(date_trunc, fmt ts);
spark_expr_fn!(time_trunc, fmt t);
spark_expr_fn!(trunc, dt fmt);
spark_expr_fn!(date_part, field source);
spark_expr_fn!(from_utc_timestamp, ts tz);
spark_expr_fn!(to_utc_timestamp, ts tz);
spark_expr_fn!(unix_date, dt);
spark_expr_fn!(unix_micros, ts);
spark_expr_fn!(unix_millis, ts);
spark_expr_fn!(unix_seconds, ts);

// ---------------------------------------------------------------------------
// Hash functions
// ---------------------------------------------------------------------------

spark_expr_fn!(crc32, arg1);
spark_expr_fn!(sha1, arg1);
spark_expr_fn!(sha2, arg1 bit_length);
spark_udf_vec!(xxhash64, udf::hash::xxhash64);

// ---------------------------------------------------------------------------
// JSON functions
// ---------------------------------------------------------------------------

spark_udf_vec!(json_tuple, udf::json::json_tuple);

// ---------------------------------------------------------------------------
// Map functions
// ---------------------------------------------------------------------------

spark_expr_fn!(map_from_arrays, keys values);
spark_expr_fn!(map_from_entries, arg1);
spark_expr_fn!(str_to_map, text pair_delim key_value_delim);

// ---------------------------------------------------------------------------
// Math functions
// ---------------------------------------------------------------------------

spark_expr_fn!(abs, arg1);
spark_expr_fn!(ceil, arg1);
spark_expr_fn!(expm1, arg1);
spark_expr_fn!(factorial, arg1);
spark_expr_fn!(floor, arg1);
spark_expr_fn!(hex, arg1);
spark_expr_fn!(modulus, dividend divisor);
spark_expr_fn!(pmod, dividend divisor);
spark_expr_fn!(rint, arg1);
spark_expr_fn!(round, value scale);
spark_expr_fn!(unhex, arg1);
spark_expr_fn!(width_bucket, value min_value max_value num_buckets);
spark_expr_fn!(csc, arg1);
spark_expr_fn!(sec, arg1);
spark_expr_fn!(negative, arg1);
spark_expr_fn!(bin, arg1);

// ---------------------------------------------------------------------------
// String functions
// ---------------------------------------------------------------------------

spark_expr_fn!(ascii, arg1);
spark_expr_fn!(base64, bin_input);
// `char` collides with the Rust primitive type in macro hygiene; rename the
// Rust ident and re-expose under the original name to Python.
#[pyfunction]
#[pyo3(name = "char")]
fn char_fn(arg1: PyExpr) -> PyExpr {
    expr_fn::char(arg1.into()).into()
}
spark_udf_vec!(concat, udf::string::concat);
spark_udf_vec!(elt, udf::string::elt);
spark_expr_fn!(ilike, str pattern);
spark_expr_fn!(length, arg1);
spark_expr_fn!(like, str pattern);
spark_expr_fn!(luhn_check, arg1);
spark_udf_vec!(format_string, udf::string::format_string);
spark_expr_fn!(space, arg1);
spark_expr_fn!(substring, str pos length);
spark_expr_fn!(unbase64, str);
spark_expr_fn!(soundex, str);
spark_expr_fn!(is_valid_utf8, str);
spark_expr_fn!(make_valid_utf8, str);

// ---------------------------------------------------------------------------
// URL functions
// ---------------------------------------------------------------------------

spark_udf_vec!(parse_url, udf::url::parse_url);
spark_udf_vec!(try_parse_url, udf::url::try_parse_url);
spark_udf_vec!(url_decode, udf::url::url_decode);
spark_udf_vec!(try_url_decode, udf::url::try_url_decode);
spark_udf_vec!(url_encode, udf::url::url_encode);

// ---------------------------------------------------------------------------
// Module init
// ---------------------------------------------------------------------------

pub(crate) fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Aggregate
    m.add_wrapped(wrap_pyfunction!(avg))?;
    m.add_wrapped(wrap_pyfunction!(try_sum))?;
    m.add_wrapped(wrap_pyfunction!(collect_list))?;
    m.add_wrapped(wrap_pyfunction!(collect_set))?;
    // Array
    m.add_wrapped(wrap_pyfunction!(array_contains))?;
    m.add_wrapped(wrap_pyfunction!(array))?;
    m.add_wrapped(wrap_pyfunction!(shuffle))?;
    m.add_wrapped(wrap_pyfunction!(array_repeat))?;
    m.add_wrapped(wrap_pyfunction!(slice))?;
    // Bitmap
    m.add_wrapped(wrap_pyfunction!(bitmap_count))?;
    m.add_wrapped(wrap_pyfunction!(bitmap_bit_position))?;
    m.add_wrapped(wrap_pyfunction!(bitmap_bucket_number))?;
    // Bitwise
    m.add_wrapped(wrap_pyfunction!(bit_get))?;
    m.add_wrapped(wrap_pyfunction!(bit_count))?;
    m.add_wrapped(wrap_pyfunction!(bitwise_not))?;
    m.add_wrapped(wrap_pyfunction!(shiftleft))?;
    m.add_wrapped(wrap_pyfunction!(shiftright))?;
    m.add_wrapped(wrap_pyfunction!(shiftrightunsigned))?;
    // Collection
    m.add_wrapped(wrap_pyfunction!(size))?;
    // Conditional
    m.add_wrapped(wrap_pyfunction!(if_))?;
    // Conversion
    m.add_wrapped(wrap_pyfunction!(spark_cast))?;
    // Datetime
    m.add_wrapped(wrap_pyfunction!(add_months))?;
    m.add_wrapped(wrap_pyfunction!(date_add))?;
    m.add_wrapped(wrap_pyfunction!(date_sub))?;
    m.add_wrapped(wrap_pyfunction!(hour))?;
    m.add_wrapped(wrap_pyfunction!(minute))?;
    m.add_wrapped(wrap_pyfunction!(second))?;
    m.add_wrapped(wrap_pyfunction!(last_day))?;
    m.add_wrapped(wrap_pyfunction!(make_dt_interval))?;
    m.add_wrapped(wrap_pyfunction!(make_interval))?;
    m.add_wrapped(wrap_pyfunction!(next_day))?;
    m.add_wrapped(wrap_pyfunction!(date_diff))?;
    m.add_wrapped(wrap_pyfunction!(date_trunc))?;
    m.add_wrapped(wrap_pyfunction!(time_trunc))?;
    m.add_wrapped(wrap_pyfunction!(trunc))?;
    m.add_wrapped(wrap_pyfunction!(date_part))?;
    m.add_wrapped(wrap_pyfunction!(from_utc_timestamp))?;
    m.add_wrapped(wrap_pyfunction!(to_utc_timestamp))?;
    m.add_wrapped(wrap_pyfunction!(unix_date))?;
    m.add_wrapped(wrap_pyfunction!(unix_micros))?;
    m.add_wrapped(wrap_pyfunction!(unix_millis))?;
    m.add_wrapped(wrap_pyfunction!(unix_seconds))?;
    // Hash
    m.add_wrapped(wrap_pyfunction!(crc32))?;
    m.add_wrapped(wrap_pyfunction!(sha1))?;
    m.add_wrapped(wrap_pyfunction!(sha2))?;
    m.add_wrapped(wrap_pyfunction!(xxhash64))?;
    // JSON
    m.add_wrapped(wrap_pyfunction!(json_tuple))?;
    // Map
    m.add_wrapped(wrap_pyfunction!(map_from_arrays))?;
    m.add_wrapped(wrap_pyfunction!(map_from_entries))?;
    m.add_wrapped(wrap_pyfunction!(str_to_map))?;
    // Math
    m.add_wrapped(wrap_pyfunction!(abs))?;
    m.add_wrapped(wrap_pyfunction!(ceil))?;
    m.add_wrapped(wrap_pyfunction!(expm1))?;
    m.add_wrapped(wrap_pyfunction!(factorial))?;
    m.add_wrapped(wrap_pyfunction!(floor))?;
    m.add_wrapped(wrap_pyfunction!(hex))?;
    m.add_wrapped(wrap_pyfunction!(modulus))?;
    m.add_wrapped(wrap_pyfunction!(pmod))?;
    m.add_wrapped(wrap_pyfunction!(rint))?;
    m.add_wrapped(wrap_pyfunction!(round))?;
    m.add_wrapped(wrap_pyfunction!(unhex))?;
    m.add_wrapped(wrap_pyfunction!(width_bucket))?;
    m.add_wrapped(wrap_pyfunction!(csc))?;
    m.add_wrapped(wrap_pyfunction!(sec))?;
    m.add_wrapped(wrap_pyfunction!(negative))?;
    m.add_wrapped(wrap_pyfunction!(bin))?;
    // String
    m.add_wrapped(wrap_pyfunction!(ascii))?;
    m.add_wrapped(wrap_pyfunction!(base64))?;
    m.add_wrapped(wrap_pyfunction!(char_fn))?;
    m.add_wrapped(wrap_pyfunction!(concat))?;
    m.add_wrapped(wrap_pyfunction!(elt))?;
    m.add_wrapped(wrap_pyfunction!(ilike))?;
    m.add_wrapped(wrap_pyfunction!(length))?;
    m.add_wrapped(wrap_pyfunction!(like))?;
    m.add_wrapped(wrap_pyfunction!(luhn_check))?;
    m.add_wrapped(wrap_pyfunction!(format_string))?;
    m.add_wrapped(wrap_pyfunction!(space))?;
    m.add_wrapped(wrap_pyfunction!(substring))?;
    m.add_wrapped(wrap_pyfunction!(unbase64))?;
    m.add_wrapped(wrap_pyfunction!(soundex))?;
    m.add_wrapped(wrap_pyfunction!(is_valid_utf8))?;
    m.add_wrapped(wrap_pyfunction!(make_valid_utf8))?;
    // URL
    m.add_wrapped(wrap_pyfunction!(parse_url))?;
    m.add_wrapped(wrap_pyfunction!(try_parse_url))?;
    m.add_wrapped(wrap_pyfunction!(url_decode))?;
    m.add_wrapped(wrap_pyfunction!(try_url_decode))?;
    m.add_wrapped(wrap_pyfunction!(url_encode))?;
    Ok(())
}
