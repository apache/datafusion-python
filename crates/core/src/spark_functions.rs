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
//! from Python under `datafusion.functions.spark`. Each scalar wrapper
//! resolves the underlying `ScalarUDF` from `datafusion_spark` and builds an
//! `Expr::ScalarFunction` directly, so behaviour matches what
//! `datafusion_spark::register_all` registers for SQL.

use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion_spark::function::{
    aggregate as fn_aggregate, array as fn_array, bitmap as fn_bitmap, bitwise as fn_bitwise,
    collection as fn_collection, conditional as fn_conditional, conversion as fn_conversion,
    datetime as fn_datetime, hash as fn_hash, json as fn_json, map as fn_map, math as fn_math,
    string as fn_string, url as fn_url,
};
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::common::data_type::NullTreatment;
use crate::errors::PyDataFusionResult;
use crate::expr::PyExpr;
use crate::expr::sort_expr::PySortExpr;
use crate::functions::add_builder_fns_to_aggregate;

/// Build an `Expr::ScalarFunction` by invoking the given `ScalarUDF` factory
/// with the supplied arguments.
macro_rules! spark_udf_fixed {
    ($PY_NAME:ident, $UDF_PATH:path, $($arg:ident),+ $(,)?) => {
        #[pyfunction]
        fn $PY_NAME($($arg: PyExpr),+) -> PyExpr {
            let udf = $UDF_PATH();
            let args: Vec<Expr> = vec![$($arg.into()),+];
            Expr::ScalarFunction(ScalarFunction::new_udf(udf, args)).into()
        }
    };
}

/// Build an `Expr::ScalarFunction` from a variadic `*args` list.
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

/// Build an aggregate `Expr` from a Spark `AggregateUDF` factory and the
/// optional builder fields (distinct/filter/order_by/null_treatment),
/// mirroring the existing `aggregate_function!` macro for default DataFusion
/// aggregates.
macro_rules! spark_aggregate_fixed {
    ($PY_NAME:ident, $UDF_PATH:path, $($arg:ident),+ $(,)?) => {
        #[pyfunction]
        #[pyo3(signature = ($($arg),+, distinct=None, filter=None, order_by=None, null_treatment=None))]
        fn $PY_NAME(
            $($arg: PyExpr),+,
            distinct: Option<bool>,
            filter: Option<PyExpr>,
            order_by: Option<Vec<PySortExpr>>,
            null_treatment: Option<NullTreatment>,
        ) -> PyDataFusionResult<PyExpr> {
            let udf = $UDF_PATH();
            let args: Vec<Expr> = vec![$($arg.into()),+];
            let agg_fn = udf.call(args);
            add_builder_fns_to_aggregate(agg_fn, distinct, filter, order_by, null_treatment)
        }
    };
}

// ---------------------------------------------------------------------------
// Aggregate functions
// ---------------------------------------------------------------------------

spark_aggregate_fixed!(avg, fn_aggregate::avg, arg1);
spark_aggregate_fixed!(try_sum, fn_aggregate::try_sum, arg1);
spark_aggregate_fixed!(collect_list, fn_aggregate::collect_list, arg1);
spark_aggregate_fixed!(collect_set, fn_aggregate::collect_set, arg1);

// ---------------------------------------------------------------------------
// Array functions
// ---------------------------------------------------------------------------

spark_udf_fixed!(
    array_contains,
    fn_array::spark_array_contains,
    array,
    element
);
spark_udf_vec!(array, fn_array::array);
spark_udf_fixed!(shuffle, fn_array::shuffle, arg1);
spark_udf_fixed!(array_repeat, fn_array::array_repeat, element, count);
spark_udf_fixed!(slice, fn_array::slice, arg_array, start, length);

// ---------------------------------------------------------------------------
// Bitmap functions
// ---------------------------------------------------------------------------

spark_udf_fixed!(bitmap_count, fn_bitmap::bitmap_count, arg1);
spark_udf_fixed!(bitmap_bit_position, fn_bitmap::bitmap_bit_position, arg1);
spark_udf_fixed!(bitmap_bucket_number, fn_bitmap::bitmap_bucket_number, arg1);

// ---------------------------------------------------------------------------
// Bitwise functions
// ---------------------------------------------------------------------------

spark_udf_fixed!(bit_get, fn_bitwise::bit_get, col, pos);
spark_udf_fixed!(bit_count, fn_bitwise::bit_count, col);
spark_udf_fixed!(bitwise_not, fn_bitwise::bitwise_not, col);
spark_udf_fixed!(shiftleft, fn_bitwise::shiftleft, value, shift);
spark_udf_fixed!(shiftright, fn_bitwise::shiftright, value, shift);
spark_udf_fixed!(
    shiftrightunsigned,
    fn_bitwise::shiftrightunsigned,
    value,
    shift
);

// ---------------------------------------------------------------------------
// Collection functions
// ---------------------------------------------------------------------------

spark_udf_fixed!(size, fn_collection::size, arg1);

// ---------------------------------------------------------------------------
// Conditional functions
// ---------------------------------------------------------------------------

// Python keyword `if` → exposed as `if_`.
spark_udf_fixed!(if_, fn_conditional::r#if, condition, if_true, if_false);

// ---------------------------------------------------------------------------
// Conversion functions
// ---------------------------------------------------------------------------

// `spark_cast` requires session ConfigOptions in upstream; use the
// crate-provided `expr_fn` helper which applies defaults.
#[pyfunction]
fn spark_cast(arg1: PyExpr, arg2: PyExpr) -> PyExpr {
    fn_conversion::expr_fn::spark_cast(arg1.into(), arg2.into()).into()
}

// ---------------------------------------------------------------------------
// Datetime functions
// ---------------------------------------------------------------------------

spark_udf_fixed!(add_months, fn_datetime::add_months, start_date, num_months);
spark_udf_fixed!(date_add, fn_datetime::date_add, start_date, days);
spark_udf_fixed!(date_sub, fn_datetime::date_sub, start_date, days);
spark_udf_fixed!(hour, fn_datetime::hour, arg1);
spark_udf_fixed!(minute, fn_datetime::minute, arg1);
spark_udf_fixed!(second, fn_datetime::second, arg1);
spark_udf_fixed!(last_day, fn_datetime::last_day, arg1);
spark_udf_fixed!(
    make_dt_interval,
    fn_datetime::make_dt_interval,
    days,
    hours,
    mins,
    secs
);
spark_udf_fixed!(
    make_interval,
    fn_datetime::make_interval,
    years,
    months,
    weeks,
    days,
    hours,
    mins,
    secs
);
spark_udf_fixed!(next_day, fn_datetime::next_day, start_date, day_of_week);
spark_udf_fixed!(date_diff, fn_datetime::date_diff, end_date, start_date);
spark_udf_fixed!(date_trunc, fn_datetime::date_trunc, fmt, ts);
spark_udf_fixed!(time_trunc, fn_datetime::time_trunc, fmt, t);
spark_udf_fixed!(trunc, fn_datetime::trunc, dt, fmt);
spark_udf_fixed!(date_part, fn_datetime::date_part, field, source);
spark_udf_fixed!(from_utc_timestamp, fn_datetime::from_utc_timestamp, ts, tz);
spark_udf_fixed!(to_utc_timestamp, fn_datetime::to_utc_timestamp, ts, tz);
spark_udf_fixed!(unix_date, fn_datetime::unix_date, dt);
spark_udf_fixed!(unix_micros, fn_datetime::unix_micros, ts);
spark_udf_fixed!(unix_millis, fn_datetime::unix_millis, ts);
spark_udf_fixed!(unix_seconds, fn_datetime::unix_seconds, ts);

// ---------------------------------------------------------------------------
// Hash functions
// ---------------------------------------------------------------------------

spark_udf_fixed!(crc32, fn_hash::crc32, arg1);
spark_udf_fixed!(sha1, fn_hash::sha1, arg1);
spark_udf_fixed!(sha2, fn_hash::sha2, arg1, bit_length);
spark_udf_vec!(xxhash64, fn_hash::xxhash64);

// ---------------------------------------------------------------------------
// JSON functions
// ---------------------------------------------------------------------------

spark_udf_vec!(json_tuple, fn_json::json_tuple);

// ---------------------------------------------------------------------------
// Map functions
// ---------------------------------------------------------------------------

spark_udf_fixed!(map_from_arrays, fn_map::map_from_arrays, keys, values);
spark_udf_fixed!(map_from_entries, fn_map::map_from_entries, arg1);
spark_udf_fixed!(
    str_to_map,
    fn_map::str_to_map,
    text,
    pair_delim,
    key_value_delim
);

// ---------------------------------------------------------------------------
// Math functions
// ---------------------------------------------------------------------------

spark_udf_fixed!(abs, fn_math::abs, arg1);
spark_udf_fixed!(ceil, fn_math::ceil, arg1);
spark_udf_fixed!(expm1, fn_math::expm1, arg1);
spark_udf_fixed!(factorial, fn_math::factorial, arg1);
spark_udf_fixed!(floor, fn_math::floor, arg1);
spark_udf_fixed!(hex, fn_math::hex, arg1);
spark_udf_fixed!(modulus, fn_math::modulus, dividend, divisor);
spark_udf_fixed!(pmod, fn_math::pmod, dividend, divisor);
spark_udf_fixed!(rint, fn_math::rint, arg1);
spark_udf_fixed!(round, fn_math::round, value, scale);
spark_udf_fixed!(unhex, fn_math::unhex, arg1);
spark_udf_fixed!(
    width_bucket,
    fn_math::width_bucket,
    value,
    min_value,
    max_value,
    num_buckets
);
spark_udf_fixed!(csc, fn_math::csc, arg1);
spark_udf_fixed!(sec, fn_math::sec, arg1);
spark_udf_fixed!(negative, fn_math::negative, arg1);
spark_udf_fixed!(bin, fn_math::bin, arg1);

// ---------------------------------------------------------------------------
// String functions
// ---------------------------------------------------------------------------

spark_udf_fixed!(ascii, fn_string::ascii, arg1);
spark_udf_fixed!(base64, fn_string::base64, bin_input);
// `char` collides with the Rust primitive type in macro hygiene; rename the
// Rust ident and re-expose under the original name to Python.
#[pyfunction]
#[pyo3(name = "char")]
fn char_fn(arg1: PyExpr) -> PyExpr {
    let udf = fn_string::char();
    Expr::ScalarFunction(ScalarFunction::new_udf(udf, vec![arg1.into()])).into()
}
spark_udf_vec!(concat, fn_string::concat);
spark_udf_vec!(elt, fn_string::elt);
spark_udf_fixed!(ilike, fn_string::ilike, str, pattern);
spark_udf_fixed!(length, fn_string::length, arg1);
spark_udf_fixed!(like, fn_string::like, str, pattern);
spark_udf_fixed!(luhn_check, fn_string::luhn_check, arg1);
spark_udf_vec!(format_string, fn_string::format_string);
spark_udf_fixed!(space, fn_string::space, arg1);
spark_udf_fixed!(substring, fn_string::substring, str, pos, length);
spark_udf_fixed!(unbase64, fn_string::unbase64, str);
spark_udf_fixed!(soundex, fn_string::soundex, str);
spark_udf_fixed!(is_valid_utf8, fn_string::is_valid_utf8, str);
spark_udf_fixed!(make_valid_utf8, fn_string::make_valid_utf8, str);

// ---------------------------------------------------------------------------
// URL functions
// ---------------------------------------------------------------------------

spark_udf_vec!(parse_url, fn_url::parse_url);
spark_udf_vec!(try_parse_url, fn_url::try_parse_url);
spark_udf_vec!(url_decode, fn_url::url_decode);
spark_udf_vec!(try_url_decode, fn_url::try_url_decode);
spark_udf_vec!(url_encode, fn_url::url_encode);

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
