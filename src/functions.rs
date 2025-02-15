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

use datafusion::functions_aggregate::all_default_aggregate_functions;
use datafusion::functions_window::all_default_window_functions;
use datafusion::logical_expr::expr::WindowFunctionParams;
use datafusion::logical_expr::ExprFunctionExt;
use datafusion::logical_expr::WindowFrame;
use pyo3::{prelude::*, wrap_pyfunction};

use crate::common::data_type::NullTreatment;
use crate::common::data_type::PyScalarValue;
use crate::context::PySessionContext;
use crate::errors::PyDataFusionError;
use crate::errors::PyDataFusionResult;
use crate::expr::conditional_expr::PyCaseBuilder;
use crate::expr::sort_expr::to_sort_expressions;
use crate::expr::sort_expr::PySortExpr;
use crate::expr::window::PyWindowFrame;
use crate::expr::PyExpr;
use datafusion::common::{Column, ScalarValue, TableReference};
use datafusion::execution::FunctionRegistry;
use datafusion::functions;
use datafusion::functions_aggregate;
use datafusion::functions_window;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::sqlparser::ast::NullTreatment as DFNullTreatment;
use datafusion::logical_expr::{expr::WindowFunction, lit, Expr, WindowFunctionDefinition};

fn add_builder_fns_to_aggregate(
    agg_fn: Expr,
    distinct: Option<bool>,
    filter: Option<PyExpr>,
    order_by: Option<Vec<PySortExpr>>,
    null_treatment: Option<NullTreatment>,
) -> PyDataFusionResult<PyExpr> {
    // Since ExprFuncBuilder::new() is private, we can guarantee initializing
    // a builder with an `null_treatment` with option None
    let mut builder = agg_fn.null_treatment(None);

    if let Some(order_by_cols) = order_by {
        let order_by_cols = to_sort_expressions(order_by_cols);
        builder = builder.order_by(order_by_cols);
    }

    if let Some(true) = distinct {
        builder = builder.distinct();
    }

    if let Some(filter) = filter {
        builder = builder.filter(filter.expr);
    }

    builder = builder.null_treatment(null_treatment.map(DFNullTreatment::from));

    Ok(builder.build()?.into())
}

#[pyfunction]
fn in_list(expr: PyExpr, value: Vec<PyExpr>, negated: bool) -> PyExpr {
    datafusion::logical_expr::in_list(
        expr.expr,
        value.into_iter().map(|x| x.expr).collect::<Vec<_>>(),
        negated,
    )
    .into()
}

#[pyfunction]
fn make_array(exprs: Vec<PyExpr>) -> PyExpr {
    datafusion::functions_nested::expr_fn::make_array(exprs.into_iter().map(|x| x.into()).collect())
        .into()
}

#[pyfunction]
fn array_concat(exprs: Vec<PyExpr>) -> PyExpr {
    let exprs = exprs.into_iter().map(|x| x.into()).collect();
    datafusion::functions_nested::expr_fn::array_concat(exprs).into()
}

#[pyfunction]
fn array_cat(exprs: Vec<PyExpr>) -> PyExpr {
    array_concat(exprs)
}

#[pyfunction]
#[pyo3(signature = (array, element, index=None))]
fn array_position(array: PyExpr, element: PyExpr, index: Option<i64>) -> PyExpr {
    let index = ScalarValue::Int64(index);
    let index = Expr::Literal(index, None);
    datafusion::functions_nested::expr_fn::array_position(array.into(), element.into(), index)
        .into()
}

#[pyfunction]
#[pyo3(signature = (array, begin, end, stride=None))]
fn array_slice(array: PyExpr, begin: PyExpr, end: PyExpr, stride: Option<PyExpr>) -> PyExpr {
    datafusion::functions_nested::expr_fn::array_slice(
        array.into(),
        begin.into(),
        end.into(),
        stride.map(Into::into),
    )
    .into()
}

/// Computes a binary hash of the given data. type is the algorithm to use.
/// Standard algorithms are md5, sha224, sha256, sha384, sha512, blake2s, blake2b, and blake3.
// #[pyfunction(value, method)]
#[pyfunction]
fn digest(value: PyExpr, method: PyExpr) -> PyExpr {
    PyExpr {
        expr: functions::expr_fn::digest(value.expr, method.expr),
    }
}

/// Concatenates the text representations of all the arguments.
/// NULL arguments are ignored.
#[pyfunction]
fn concat(args: Vec<PyExpr>) -> PyResult<PyExpr> {
    let args = args.into_iter().map(|e| e.expr).collect::<Vec<_>>();
    Ok(functions::string::expr_fn::concat(args).into())
}

/// Concatenates all but the first argument, with separators.
/// The first argument is used as the separator string, and should not be NULL.
/// Other NULL arguments are ignored.
#[pyfunction]
fn concat_ws(sep: String, args: Vec<PyExpr>) -> PyResult<PyExpr> {
    let args = args.into_iter().map(|e| e.expr).collect::<Vec<_>>();
    Ok(functions::string::expr_fn::concat_ws(lit(sep), args).into())
}

#[pyfunction]
#[pyo3(signature = (values, regex, flags=None))]
fn regexp_like(values: PyExpr, regex: PyExpr, flags: Option<PyExpr>) -> PyResult<PyExpr> {
    Ok(functions::expr_fn::regexp_like(values.expr, regex.expr, flags.map(|x| x.expr)).into())
}

#[pyfunction]
#[pyo3(signature = (values, regex, flags=None))]
fn regexp_match(values: PyExpr, regex: PyExpr, flags: Option<PyExpr>) -> PyResult<PyExpr> {
    Ok(functions::expr_fn::regexp_match(values.expr, regex.expr, flags.map(|x| x.expr)).into())
}

#[pyfunction]
#[pyo3(signature = (string, pattern, replacement, flags=None))]
/// Replaces substring(s) matching a POSIX regular expression.
fn regexp_replace(
    string: PyExpr,
    pattern: PyExpr,
    replacement: PyExpr,
    flags: Option<PyExpr>,
) -> PyResult<PyExpr> {
    Ok(functions::expr_fn::regexp_replace(
        string.into(),
        pattern.into(),
        replacement.into(),
        flags.map(|x| x.expr),
    )
    .into())
}

#[pyfunction]
#[pyo3(signature = (string, pattern, start, flags=None))]
/// Returns the number of matches found in the string.
fn regexp_count(
    string: PyExpr,
    pattern: PyExpr,
    start: Option<PyExpr>,
    flags: Option<PyExpr>,
) -> PyResult<PyExpr> {
    Ok(functions::expr_fn::regexp_count(
        string.expr,
        pattern.expr,
        start.map(|x| x.expr),
        flags.map(|x| x.expr),
    )
    .into())
}

/// Creates a new Sort Expr
#[pyfunction]
fn order_by(expr: PyExpr, asc: bool, nulls_first: bool) -> PyResult<PySortExpr> {
    Ok(PySortExpr::from(datafusion::logical_expr::expr::Sort {
        expr: expr.expr,
        asc,
        nulls_first,
    }))
}

/// Creates a new Alias Expr
#[pyfunction]
#[pyo3(signature = (expr, name, metadata=None))]
fn alias(expr: PyExpr, name: &str, metadata: Option<HashMap<String, String>>) -> PyResult<PyExpr> {
    let relation: Option<TableReference> = None;
    Ok(PyExpr {
        expr: datafusion::logical_expr::Expr::Alias(
            Alias::new(expr.expr, relation, name).with_metadata(metadata),
        ),
    })
}

/// Create a column reference Expr
#[pyfunction]
fn col(name: &str) -> PyResult<PyExpr> {
    Ok(PyExpr {
        expr: datafusion::logical_expr::Expr::Column(Column::new_unqualified(name)),
    })
}

/// Create a CASE WHEN statement with literal WHEN expressions for comparison to the base expression.
#[pyfunction]
fn case(expr: PyExpr) -> PyResult<PyCaseBuilder> {
    Ok(PyCaseBuilder {
        case_builder: datafusion::logical_expr::case(expr.expr),
    })
}

/// Create a CASE WHEN statement with literal WHEN expressions for comparison to the base expression.
#[pyfunction]
fn when(when: PyExpr, then: PyExpr) -> PyResult<PyCaseBuilder> {
    Ok(PyCaseBuilder {
        case_builder: datafusion::logical_expr::when(when.expr, then.expr),
    })
}

/// Helper function to find the appropriate window function.
///
/// Search procedure:
/// 1) Search built in window functions, which are being deprecated.
/// 1) If a session context is provided:
///      1) search User Defined Aggregate Functions (UDAFs)
///      1) search registered window functions
///      1) search registered aggregate functions
/// 1) If no function has been found, search default aggregate functions.
///
/// NOTE: we search the built-ins first because the `UDAF` versions currently do not have the same behavior.
fn find_window_fn(
    name: &str,
    ctx: Option<PySessionContext>,
) -> PyDataFusionResult<WindowFunctionDefinition> {
    if let Some(ctx) = ctx {
        // search UDAFs
        let udaf = ctx
            .ctx
            .udaf(name)
            .map(WindowFunctionDefinition::AggregateUDF)
            .ok();

        if let Some(udaf) = udaf {
            return Ok(udaf);
        }

        let session_state = ctx.ctx.state();

        // search registered window functions
        let window_fn = session_state
            .window_functions()
            .get(name)
            .map(|f| WindowFunctionDefinition::WindowUDF(f.clone()));

        if let Some(window_fn) = window_fn {
            return Ok(window_fn);
        }

        // search registered aggregate functions
        let agg_fn = session_state
            .aggregate_functions()
            .get(name)
            .map(|f| WindowFunctionDefinition::AggregateUDF(f.clone()));

        if let Some(agg_fn) = agg_fn {
            return Ok(agg_fn);
        }
    }

    // search default aggregate functions
    let agg_fn = all_default_aggregate_functions()
        .iter()
        .find(|v| v.name() == name || v.aliases().contains(&name.to_string()))
        .map(|f| WindowFunctionDefinition::AggregateUDF(f.clone()));

    if let Some(agg_fn) = agg_fn {
        return Ok(agg_fn);
    }

    // search default window functions
    let window_fn = all_default_window_functions()
        .iter()
        .find(|v| v.name() == name || v.aliases().contains(&name.to_string()))
        .map(|f| WindowFunctionDefinition::WindowUDF(f.clone()));

    if let Some(window_fn) = window_fn {
        return Ok(window_fn);
    }

    Err(PyDataFusionError::Common(format!(
        "window function `{name}` not found"
    )))
}

/// Creates a new Window function expression
#[pyfunction]
#[pyo3(signature = (name, args, partition_by=None, order_by=None, window_frame=None, ctx=None))]
fn window(
    name: &str,
    args: Vec<PyExpr>,
    partition_by: Option<Vec<PyExpr>>,
    order_by: Option<Vec<PySortExpr>>,
    window_frame: Option<PyWindowFrame>,
    ctx: Option<PySessionContext>,
) -> PyResult<PyExpr> {
    let fun = find_window_fn(name, ctx)?;

    let window_frame = window_frame
        .map(|w| w.into())
        .unwrap_or(WindowFrame::new(order_by.as_ref().map(|v| !v.is_empty())));

    Ok(PyExpr {
        expr: datafusion::logical_expr::Expr::WindowFunction(Box::new(WindowFunction {
            fun,
            params: WindowFunctionParams {
                args: args.into_iter().map(|x| x.expr).collect::<Vec<_>>(),
                partition_by: partition_by
                    .unwrap_or_default()
                    .into_iter()
                    .map(|x| x.expr)
                    .collect::<Vec<_>>(),
                order_by: order_by
                    .unwrap_or_default()
                    .into_iter()
                    .map(|x| x.into())
                    .collect::<Vec<_>>(),
                window_frame,
                null_treatment: None,
            },
        })),
    })
}

// Generates a [pyo3] wrapper for associated aggregate functions.
// All of the builder options are exposed to the python internal
// function and we rely on the wrappers to only use those that
// are appropriate.
macro_rules! aggregate_function {
    ($NAME: ident) => {
        aggregate_function!($NAME, expr);
    };
    ($NAME: ident, $($arg:ident)*) => {
        #[pyfunction]
        #[pyo3(signature = ($($arg),*, distinct=None, filter=None, order_by=None, null_treatment=None))]
        fn $NAME(
            $($arg: PyExpr),*,
            distinct: Option<bool>,
            filter: Option<PyExpr>,
            order_by: Option<Vec<PySortExpr>>,
            null_treatment: Option<NullTreatment>
        ) -> PyDataFusionResult<PyExpr> {
            let agg_fn = functions_aggregate::expr_fn::$NAME($($arg.into()),*);

            add_builder_fns_to_aggregate(agg_fn, distinct, filter, order_by, null_treatment)
        }
    };
}

/// Generates a [pyo3] wrapper for [datafusion::functions::expr_fn]
///
/// These functions have explicit named arguments.
macro_rules! expr_fn {
    ($FUNC: ident) => {
        expr_fn!($FUNC, , stringify!($FUNC));
    };
    ($FUNC:ident, $($arg:ident)*) => {
        expr_fn!($FUNC, $($arg)*, stringify!($FUNC));
    };
    ($FUNC: ident, $DOC: expr) => {
        expr_fn!($FUNC, ,$DOC);
    };
    ($FUNC: ident, $($arg:ident)*, $DOC: expr) => {
        #[doc = $DOC]
        #[pyfunction]
        fn $FUNC($($arg: PyExpr),*) -> PyExpr {
            functions::expr_fn::$FUNC($($arg.into()),*).into()
        }
    };
}
/// Generates a [pyo3] wrapper for [datafusion::functions::expr_fn]
///
/// These functions take a single `Vec<PyExpr>` argument using `pyo3(signature = (*args))`.
macro_rules! expr_fn_vec {
    ($FUNC: ident) => {
        expr_fn_vec!($FUNC, stringify!($FUNC));
    };
    ($FUNC: ident, $DOC: expr) => {
        #[doc = $DOC]
        #[pyfunction]
        #[pyo3(signature = (*args))]
        fn $FUNC(args: Vec<PyExpr>) -> PyExpr {
            let args = args.into_iter().map(|e| e.into()).collect::<Vec<_>>();
            functions::expr_fn::$FUNC(args).into()
        }
    };
}

/// Generates a [pyo3] wrapper for [datafusion_functions_nested::expr_fn]
///
/// These functions have explicit named arguments.
macro_rules! array_fn {
    ($FUNC: ident) => {
        array_fn!($FUNC, , stringify!($FUNC));
    };
    ($FUNC:ident,  $($arg:ident)*) => {
        array_fn!($FUNC, $($arg)*, stringify!($FUNC));
    };
    ($FUNC: ident, $DOC: expr) => {
        array_fn!($FUNC, , $DOC);
    };
    ($FUNC: ident, $($arg:ident)*, $DOC:expr) => {
        #[doc = $DOC]
        #[pyfunction]
        fn $FUNC($($arg: PyExpr),*) -> PyExpr {
            datafusion::functions_nested::expr_fn::$FUNC($($arg.into()),*).into()
        }
    };
}

expr_fn!(abs, num);
expr_fn!(acos, num);
expr_fn!(acosh, num);
expr_fn!(ascii, arg1, "Returns the numeric code of the first character of the argument. In UTF8 encoding, returns the Unicode code point of the character. In other multibyte encodings, the argument must be an ASCII character.");
expr_fn!(asin, num);
expr_fn!(asinh, num);
expr_fn!(atan, num);
expr_fn!(atanh, num);
expr_fn!(atan2, y x);
expr_fn!(
    bit_length,
    arg,
    "Returns number of bits in the string (8 times the octet_length)."
);
expr_fn_vec!(btrim, "Removes the longest string containing only characters in characters (a space by default) from the start and end of string.");
expr_fn!(cbrt, num);
expr_fn!(ceil, num);
expr_fn!(
    character_length,
    string,
    "Returns number of characters in the string."
);
expr_fn!(length, string);
expr_fn!(char_length, string);
expr_fn!(chr, arg, "Returns the character with the given code.");
expr_fn_vec!(coalesce);
expr_fn!(cos, num);
expr_fn!(cosh, num);
expr_fn!(cot, num);
expr_fn!(degrees, num);
expr_fn!(decode, input encoding);
expr_fn!(encode, input encoding);
expr_fn!(ends_with, string suffix, "Returns true if string ends with suffix.");
expr_fn!(exp, num);
expr_fn!(factorial, num);
expr_fn!(floor, num);
expr_fn!(gcd, x y);
expr_fn!(initcap, string, "Converts the first letter of each word to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.");
expr_fn!(isnan, num);
expr_fn!(iszero, num);
expr_fn!(levenshtein, string1 string2);
expr_fn!(lcm, x y);
expr_fn!(left, string n, "Returns first n characters in the string, or when n is negative, returns all but last |n| characters.");
expr_fn!(ln, num);
expr_fn!(log, base num);
expr_fn!(log10, num);
expr_fn!(log2, num);
expr_fn!(lower, arg1, "Converts the string to all lower case");
expr_fn_vec!(lpad, "Extends the string to length length by prepending the characters fill (a space by default). If the string is already longer than length then it is truncated (on the right).");
expr_fn_vec!(ltrim, "Removes the longest string containing only characters in characters (a space by default) from the start of string.");
expr_fn!(
    md5,
    input_arg,
    "Computes the MD5 hash of the argument, with the result written in hexadecimal."
);
expr_fn!(
    nanvl,
    x y,
    "Returns x if x is not NaN otherwise returns y."
);
expr_fn!(
    nvl,
    x y,
    "Returns x if x is not NULL otherwise returns y."
);
expr_fn!(nullif, arg_1 arg_2);
expr_fn!(octet_length, args, "Returns number of bytes in the string. Since this version of the function accepts type character directly, it will not strip trailing spaces.");
expr_fn_vec!(overlay);
expr_fn!(pi);
expr_fn!(power, base exponent);
expr_fn!(radians, num);
expr_fn!(repeat, string n, "Repeats string the specified number of times.");
expr_fn!(
    replace,
    string from to,
    "Replaces all occurrences in string of substring from with substring to."
);
expr_fn!(
    reverse,
    string,
    "Reverses the order of the characters in the string."
);
expr_fn!(right, string n, "Returns last n characters in the string, or when n is negative, returns all but first |n| characters.");
expr_fn_vec!(round);
expr_fn_vec!(rpad, "Extends the string to length length by appending the characters fill (a space by default). If the string is already longer than length then it is truncated.");
expr_fn_vec!(rtrim, "Removes the longest string containing only characters in characters (a space by default) from the end of string.");
expr_fn!(sha224, input_arg1);
expr_fn!(sha256, input_arg1);
expr_fn!(sha384, input_arg1);
expr_fn!(sha512, input_arg1);
expr_fn!(signum, num);
expr_fn!(sin, num);
expr_fn!(sinh, num);
expr_fn!(
    split_part,
    string delimiter index,
    "Splits string at occurrences of delimiter and returns the n'th field (counting from one)."
);
expr_fn!(sqrt, num);
expr_fn!(starts_with, string prefix, "Returns true if string starts with prefix.");
expr_fn!(strpos, string substring, "Returns starting index of specified substring within string, or zero if it's not present. (Same as position(substring in string), but note the reversed argument order.)");
expr_fn!(substr, string position);
expr_fn!(substr_index, string delimiter count);
expr_fn!(substring, string position length);
expr_fn!(find_in_set, string string_list);
expr_fn!(tan, num);
expr_fn!(tanh, num);
expr_fn!(
    to_hex,
    arg1,
    "Converts the number to its equivalent hexadecimal representation."
);
expr_fn!(now);
expr_fn_vec!(to_timestamp);
expr_fn_vec!(to_timestamp_millis);
expr_fn_vec!(to_timestamp_nanos);
expr_fn_vec!(to_timestamp_micros);
expr_fn_vec!(to_timestamp_seconds);
expr_fn_vec!(to_unixtime);
expr_fn!(current_date);
expr_fn!(current_time);
expr_fn!(date_part, part date);
expr_fn!(date_trunc, part date);
expr_fn!(date_bin, stride source origin);
expr_fn!(make_date, year month day);

expr_fn!(translate, string from to, "Replaces each character in string that matches a character in the from set with the corresponding character in the to set. If from is longer than to, occurrences of the extra characters in from are deleted.");
expr_fn_vec!(trim, "Removes the longest string containing only characters in characters (a space by default) from the start, end, or both ends (BOTH is the default) of string.");
expr_fn_vec!(trunc);
expr_fn!(upper, arg1, "Converts the string to all upper case.");
expr_fn!(uuid);
expr_fn_vec!(r#struct); // Use raw identifier since struct is a keyword
expr_fn_vec!(named_struct);
expr_fn!(from_unixtime, unixtime);
expr_fn!(arrow_typeof, arg_1);
expr_fn!(arrow_cast, arg_1 datatype);
expr_fn!(random);

// Array Functions
array_fn!(array_append, array element);
array_fn!(array_to_string, array delimiter);
array_fn!(array_dims, array);
array_fn!(array_distinct, array);
array_fn!(array_element, array element);
array_fn!(array_empty, array);
array_fn!(array_length, array);
array_fn!(array_has, first_array second_array);
array_fn!(array_has_all, first_array second_array);
array_fn!(array_has_any, first_array second_array);
array_fn!(array_positions, array element);
array_fn!(array_ndims, array);
array_fn!(array_prepend, element array);
array_fn!(array_pop_back, array);
array_fn!(array_pop_front, array);
array_fn!(array_remove, array element);
array_fn!(array_remove_n, array element max);
array_fn!(array_remove_all, array element);
array_fn!(array_repeat, element count);
array_fn!(array_replace, array from to);
array_fn!(array_replace_n, array from to max);
array_fn!(array_replace_all, array from to);
array_fn!(array_sort, array desc null_first);
array_fn!(array_intersect, first_array second_array);
array_fn!(array_union, array1 array2);
array_fn!(array_except, first_array second_array);
array_fn!(array_resize, array size value);
array_fn!(cardinality, array);
array_fn!(flatten, array);
array_fn!(range, start stop step);

aggregate_function!(array_agg);
aggregate_function!(max);
aggregate_function!(min);
aggregate_function!(avg);
aggregate_function!(sum);
aggregate_function!(bit_and);
aggregate_function!(bit_or);
aggregate_function!(bit_xor);
aggregate_function!(bool_and);
aggregate_function!(bool_or);
aggregate_function!(corr, y x);
aggregate_function!(count);
aggregate_function!(covar_samp, y x);
aggregate_function!(covar_pop, y x);
aggregate_function!(median);
aggregate_function!(regr_slope, y x);
aggregate_function!(regr_intercept, y x);
aggregate_function!(regr_count, y x);
aggregate_function!(regr_r2, y x);
aggregate_function!(regr_avgx, y x);
aggregate_function!(regr_avgy, y x);
aggregate_function!(regr_sxx, y x);
aggregate_function!(regr_syy, y x);
aggregate_function!(regr_sxy, y x);
aggregate_function!(stddev);
aggregate_function!(stddev_pop);
aggregate_function!(var_sample);
aggregate_function!(var_pop);
aggregate_function!(approx_distinct);
aggregate_function!(approx_median);

// Code is commented out since grouping is not yet implemented
// https://github.com/apache/datafusion-python/issues/861
// aggregate_function!(grouping);

#[pyfunction]
#[pyo3(signature = (expression, percentile, num_centroids=None, filter=None))]
pub fn approx_percentile_cont(
    expression: PyExpr,
    percentile: f64,
    num_centroids: Option<i64>, // enforces optional arguments at the end, currently
    filter: Option<PyExpr>,
) -> PyDataFusionResult<PyExpr> {
    let args = if let Some(num_centroids) = num_centroids {
        vec![expression.expr, lit(percentile), lit(num_centroids)]
    } else {
        vec![expression.expr, lit(percentile)]
    };
    let udaf = functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf();
    let agg_fn = udaf.call(args);

    add_builder_fns_to_aggregate(agg_fn, None, filter, None, None)
}

#[pyfunction]
#[pyo3(signature = (expression, weight, percentile, filter=None))]
pub fn approx_percentile_cont_with_weight(
    expression: PyExpr,
    weight: PyExpr,
    percentile: f64,
    filter: Option<PyExpr>,
) -> PyDataFusionResult<PyExpr> {
    let agg_fn = functions_aggregate::expr_fn::approx_percentile_cont_with_weight(
        expression.expr,
        weight.expr,
        lit(percentile),
    );

    add_builder_fns_to_aggregate(agg_fn, None, filter, None, None)
}

// We handle last_value explicitly because the signature expects an order_by
// https://github.com/apache/datafusion/issues/12376
#[pyfunction]
#[pyo3(signature = (expr, distinct=None, filter=None, order_by=None, null_treatment=None))]
pub fn last_value(
    expr: PyExpr,
    distinct: Option<bool>,
    filter: Option<PyExpr>,
    order_by: Option<Vec<PySortExpr>>,
    null_treatment: Option<NullTreatment>,
) -> PyDataFusionResult<PyExpr> {
    // If we initialize the UDAF with order_by directly, then it gets over-written by the builder
    let agg_fn = functions_aggregate::expr_fn::last_value(expr.expr, None);

    add_builder_fns_to_aggregate(agg_fn, distinct, filter, order_by, null_treatment)
}
// We handle first_value explicitly because the signature expects an order_by
// https://github.com/apache/datafusion/issues/12376
#[pyfunction]
#[pyo3(signature = (expr, distinct=None, filter=None, order_by=None, null_treatment=None))]
pub fn first_value(
    expr: PyExpr,
    distinct: Option<bool>,
    filter: Option<PyExpr>,
    order_by: Option<Vec<PySortExpr>>,
    null_treatment: Option<NullTreatment>,
) -> PyDataFusionResult<PyExpr> {
    // If we initialize the UDAF with order_by directly, then it gets over-written by the builder
    let agg_fn = functions_aggregate::expr_fn::first_value(expr.expr, None);

    add_builder_fns_to_aggregate(agg_fn, distinct, filter, order_by, null_treatment)
}

// nth_value requires a non-expr argument
#[pyfunction]
#[pyo3(signature = (expr, n, distinct=None, filter=None, order_by=None, null_treatment=None))]
pub fn nth_value(
    expr: PyExpr,
    n: i64,
    distinct: Option<bool>,
    filter: Option<PyExpr>,
    order_by: Option<Vec<PySortExpr>>,
    null_treatment: Option<NullTreatment>,
) -> PyDataFusionResult<PyExpr> {
    let agg_fn = datafusion::functions_aggregate::nth_value::nth_value(expr.expr, n, vec![]);
    add_builder_fns_to_aggregate(agg_fn, distinct, filter, order_by, null_treatment)
}

// string_agg requires a non-expr argument
#[pyfunction]
#[pyo3(signature = (expr, delimiter, distinct=None, filter=None, order_by=None, null_treatment=None))]
pub fn string_agg(
    expr: PyExpr,
    delimiter: String,
    distinct: Option<bool>,
    filter: Option<PyExpr>,
    order_by: Option<Vec<PySortExpr>>,
    null_treatment: Option<NullTreatment>,
) -> PyDataFusionResult<PyExpr> {
    let agg_fn = datafusion::functions_aggregate::string_agg::string_agg(expr.expr, lit(delimiter));
    add_builder_fns_to_aggregate(agg_fn, distinct, filter, order_by, null_treatment)
}

pub(crate) fn add_builder_fns_to_window(
    window_fn: Expr,
    partition_by: Option<Vec<PyExpr>>,
    window_frame: Option<PyWindowFrame>,
    order_by: Option<Vec<PySortExpr>>,
    null_treatment: Option<NullTreatment>,
) -> PyDataFusionResult<PyExpr> {
    let null_treatment = null_treatment.map(|n| n.into());
    let mut builder = window_fn.null_treatment(null_treatment);

    if let Some(partition_cols) = partition_by {
        builder = builder.partition_by(
            partition_cols
                .into_iter()
                .map(|col| col.clone().into())
                .collect(),
        );
    }

    if let Some(order_by_cols) = order_by {
        let order_by_cols = to_sort_expressions(order_by_cols);
        builder = builder.order_by(order_by_cols);
    }

    if let Some(window_frame) = window_frame {
        builder = builder.window_frame(window_frame.into());
    }

    Ok(builder.build().map(|e| e.into())?)
}

#[pyfunction]
#[pyo3(signature = (arg, shift_offset, default_value=None, partition_by=None, order_by=None))]
pub fn lead(
    arg: PyExpr,
    shift_offset: i64,
    default_value: Option<PyScalarValue>,
    partition_by: Option<Vec<PyExpr>>,
    order_by: Option<Vec<PySortExpr>>,
) -> PyDataFusionResult<PyExpr> {
    let default_value = default_value.map(|v| v.into());
    let window_fn = functions_window::expr_fn::lead(arg.expr, Some(shift_offset), default_value);

    add_builder_fns_to_window(window_fn, partition_by, None, order_by, None)
}

#[pyfunction]
#[pyo3(signature = (arg, shift_offset, default_value=None, partition_by=None, order_by=None))]
pub fn lag(
    arg: PyExpr,
    shift_offset: i64,
    default_value: Option<PyScalarValue>,
    partition_by: Option<Vec<PyExpr>>,
    order_by: Option<Vec<PySortExpr>>,
) -> PyDataFusionResult<PyExpr> {
    let default_value = default_value.map(|v| v.into());
    let window_fn = functions_window::expr_fn::lag(arg.expr, Some(shift_offset), default_value);

    add_builder_fns_to_window(window_fn, partition_by, None, order_by, None)
}

#[pyfunction]
#[pyo3(signature = (partition_by=None, order_by=None))]
pub fn row_number(
    partition_by: Option<Vec<PyExpr>>,
    order_by: Option<Vec<PySortExpr>>,
) -> PyDataFusionResult<PyExpr> {
    let window_fn = functions_window::expr_fn::row_number();

    add_builder_fns_to_window(window_fn, partition_by, None, order_by, None)
}

#[pyfunction]
#[pyo3(signature = (partition_by=None, order_by=None))]
pub fn rank(
    partition_by: Option<Vec<PyExpr>>,
    order_by: Option<Vec<PySortExpr>>,
) -> PyDataFusionResult<PyExpr> {
    let window_fn = functions_window::expr_fn::rank();

    add_builder_fns_to_window(window_fn, partition_by, None, order_by, None)
}

#[pyfunction]
#[pyo3(signature = (partition_by=None, order_by=None))]
pub fn dense_rank(
    partition_by: Option<Vec<PyExpr>>,
    order_by: Option<Vec<PySortExpr>>,
) -> PyDataFusionResult<PyExpr> {
    let window_fn = functions_window::expr_fn::dense_rank();

    add_builder_fns_to_window(window_fn, partition_by, None, order_by, None)
}

#[pyfunction]
#[pyo3(signature = (partition_by=None, order_by=None))]
pub fn percent_rank(
    partition_by: Option<Vec<PyExpr>>,
    order_by: Option<Vec<PySortExpr>>,
) -> PyDataFusionResult<PyExpr> {
    let window_fn = functions_window::expr_fn::percent_rank();

    add_builder_fns_to_window(window_fn, partition_by, None, order_by, None)
}

#[pyfunction]
#[pyo3(signature = (partition_by=None, order_by=None))]
pub fn cume_dist(
    partition_by: Option<Vec<PyExpr>>,
    order_by: Option<Vec<PySortExpr>>,
) -> PyDataFusionResult<PyExpr> {
    let window_fn = functions_window::expr_fn::cume_dist();

    add_builder_fns_to_window(window_fn, partition_by, None, order_by, None)
}

#[pyfunction]
#[pyo3(signature = (arg, partition_by=None, order_by=None))]
pub fn ntile(
    arg: PyExpr,
    partition_by: Option<Vec<PyExpr>>,
    order_by: Option<Vec<PySortExpr>>,
) -> PyDataFusionResult<PyExpr> {
    let window_fn = functions_window::expr_fn::ntile(arg.into());

    add_builder_fns_to_window(window_fn, partition_by, None, order_by, None)
}

pub(crate) fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(abs))?;
    m.add_wrapped(wrap_pyfunction!(acos))?;
    m.add_wrapped(wrap_pyfunction!(acosh))?;
    m.add_wrapped(wrap_pyfunction!(approx_distinct))?;
    m.add_wrapped(wrap_pyfunction!(alias))?;
    m.add_wrapped(wrap_pyfunction!(approx_median))?;
    m.add_wrapped(wrap_pyfunction!(approx_percentile_cont))?;
    m.add_wrapped(wrap_pyfunction!(approx_percentile_cont_with_weight))?;
    m.add_wrapped(wrap_pyfunction!(range))?;
    m.add_wrapped(wrap_pyfunction!(array_agg))?;
    m.add_wrapped(wrap_pyfunction!(arrow_typeof))?;
    m.add_wrapped(wrap_pyfunction!(arrow_cast))?;
    m.add_wrapped(wrap_pyfunction!(ascii))?;
    m.add_wrapped(wrap_pyfunction!(asin))?;
    m.add_wrapped(wrap_pyfunction!(asinh))?;
    m.add_wrapped(wrap_pyfunction!(atan))?;
    m.add_wrapped(wrap_pyfunction!(atanh))?;
    m.add_wrapped(wrap_pyfunction!(atan2))?;
    m.add_wrapped(wrap_pyfunction!(avg))?;
    m.add_wrapped(wrap_pyfunction!(bit_length))?;
    m.add_wrapped(wrap_pyfunction!(btrim))?;
    m.add_wrapped(wrap_pyfunction!(cbrt))?;
    m.add_wrapped(wrap_pyfunction!(ceil))?;
    m.add_wrapped(wrap_pyfunction!(character_length))?;
    m.add_wrapped(wrap_pyfunction!(chr))?;
    m.add_wrapped(wrap_pyfunction!(char_length))?;
    m.add_wrapped(wrap_pyfunction!(coalesce))?;
    m.add_wrapped(wrap_pyfunction!(case))?;
    m.add_wrapped(wrap_pyfunction!(when))?;
    m.add_wrapped(wrap_pyfunction!(col))?;
    m.add_wrapped(wrap_pyfunction!(concat_ws))?;
    m.add_wrapped(wrap_pyfunction!(concat))?;
    m.add_wrapped(wrap_pyfunction!(corr))?;
    m.add_wrapped(wrap_pyfunction!(cos))?;
    m.add_wrapped(wrap_pyfunction!(cosh))?;
    m.add_wrapped(wrap_pyfunction!(cot))?;
    m.add_wrapped(wrap_pyfunction!(count))?;
    m.add_wrapped(wrap_pyfunction!(covar_pop))?;
    m.add_wrapped(wrap_pyfunction!(covar_samp))?;
    m.add_wrapped(wrap_pyfunction!(current_date))?;
    m.add_wrapped(wrap_pyfunction!(current_time))?;
    m.add_wrapped(wrap_pyfunction!(degrees))?;
    m.add_wrapped(wrap_pyfunction!(date_bin))?;
    m.add_wrapped(wrap_pyfunction!(date_part))?;
    m.add_wrapped(wrap_pyfunction!(date_trunc))?;
    m.add_wrapped(wrap_pyfunction!(make_date))?;
    m.add_wrapped(wrap_pyfunction!(digest))?;
    m.add_wrapped(wrap_pyfunction!(ends_with))?;
    m.add_wrapped(wrap_pyfunction!(exp))?;
    m.add_wrapped(wrap_pyfunction!(factorial))?;
    m.add_wrapped(wrap_pyfunction!(floor))?;
    m.add_wrapped(wrap_pyfunction!(from_unixtime))?;
    m.add_wrapped(wrap_pyfunction!(gcd))?;
    // m.add_wrapped(wrap_pyfunction!(grouping))?;
    m.add_wrapped(wrap_pyfunction!(in_list))?;
    m.add_wrapped(wrap_pyfunction!(initcap))?;
    m.add_wrapped(wrap_pyfunction!(isnan))?;
    m.add_wrapped(wrap_pyfunction!(iszero))?;
    m.add_wrapped(wrap_pyfunction!(levenshtein))?;
    m.add_wrapped(wrap_pyfunction!(lcm))?;
    m.add_wrapped(wrap_pyfunction!(left))?;
    m.add_wrapped(wrap_pyfunction!(length))?;
    m.add_wrapped(wrap_pyfunction!(ln))?;
    m.add_wrapped(wrap_pyfunction!(self::log))?;
    m.add_wrapped(wrap_pyfunction!(log10))?;
    m.add_wrapped(wrap_pyfunction!(log2))?;
    m.add_wrapped(wrap_pyfunction!(lower))?;
    m.add_wrapped(wrap_pyfunction!(lpad))?;
    m.add_wrapped(wrap_pyfunction!(ltrim))?;
    m.add_wrapped(wrap_pyfunction!(max))?;
    m.add_wrapped(wrap_pyfunction!(make_array))?;
    m.add_wrapped(wrap_pyfunction!(md5))?;
    m.add_wrapped(wrap_pyfunction!(median))?;
    m.add_wrapped(wrap_pyfunction!(min))?;
    m.add_wrapped(wrap_pyfunction!(named_struct))?;
    m.add_wrapped(wrap_pyfunction!(nanvl))?;
    m.add_wrapped(wrap_pyfunction!(nvl))?;
    m.add_wrapped(wrap_pyfunction!(now))?;
    m.add_wrapped(wrap_pyfunction!(nullif))?;
    m.add_wrapped(wrap_pyfunction!(octet_length))?;
    m.add_wrapped(wrap_pyfunction!(order_by))?;
    m.add_wrapped(wrap_pyfunction!(overlay))?;
    m.add_wrapped(wrap_pyfunction!(pi))?;
    m.add_wrapped(wrap_pyfunction!(power))?;
    m.add_wrapped(wrap_pyfunction!(radians))?;
    m.add_wrapped(wrap_pyfunction!(random))?;
    m.add_wrapped(wrap_pyfunction!(regexp_count))?;
    m.add_wrapped(wrap_pyfunction!(regexp_like))?;
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
    m.add_wrapped(wrap_pyfunction!(sinh))?;
    m.add_wrapped(wrap_pyfunction!(split_part))?;
    m.add_wrapped(wrap_pyfunction!(sqrt))?;
    m.add_wrapped(wrap_pyfunction!(starts_with))?;
    m.add_wrapped(wrap_pyfunction!(stddev))?;
    m.add_wrapped(wrap_pyfunction!(stddev_pop))?;
    m.add_wrapped(wrap_pyfunction!(string_agg))?;
    m.add_wrapped(wrap_pyfunction!(strpos))?;
    m.add_wrapped(wrap_pyfunction!(r#struct))?; // Use raw identifier since struct is a keyword
    m.add_wrapped(wrap_pyfunction!(substr))?;
    m.add_wrapped(wrap_pyfunction!(substr_index))?;
    m.add_wrapped(wrap_pyfunction!(substring))?;
    m.add_wrapped(wrap_pyfunction!(find_in_set))?;
    m.add_wrapped(wrap_pyfunction!(sum))?;
    m.add_wrapped(wrap_pyfunction!(tan))?;
    m.add_wrapped(wrap_pyfunction!(tanh))?;
    m.add_wrapped(wrap_pyfunction!(to_hex))?;
    m.add_wrapped(wrap_pyfunction!(to_timestamp))?;
    m.add_wrapped(wrap_pyfunction!(to_timestamp_millis))?;
    m.add_wrapped(wrap_pyfunction!(to_timestamp_nanos))?;
    m.add_wrapped(wrap_pyfunction!(to_timestamp_micros))?;
    m.add_wrapped(wrap_pyfunction!(to_timestamp_seconds))?;
    m.add_wrapped(wrap_pyfunction!(to_unixtime))?;
    m.add_wrapped(wrap_pyfunction!(translate))?;
    m.add_wrapped(wrap_pyfunction!(trim))?;
    m.add_wrapped(wrap_pyfunction!(trunc))?;
    m.add_wrapped(wrap_pyfunction!(upper))?;
    m.add_wrapped(wrap_pyfunction!(self::uuid))?; // Use self to avoid name collision
    m.add_wrapped(wrap_pyfunction!(var_pop))?;
    m.add_wrapped(wrap_pyfunction!(var_sample))?;
    m.add_wrapped(wrap_pyfunction!(window))?;
    m.add_wrapped(wrap_pyfunction!(regr_avgx))?;
    m.add_wrapped(wrap_pyfunction!(regr_avgy))?;
    m.add_wrapped(wrap_pyfunction!(regr_count))?;
    m.add_wrapped(wrap_pyfunction!(regr_intercept))?;
    m.add_wrapped(wrap_pyfunction!(regr_r2))?;
    m.add_wrapped(wrap_pyfunction!(regr_slope))?;
    m.add_wrapped(wrap_pyfunction!(regr_sxx))?;
    m.add_wrapped(wrap_pyfunction!(regr_sxy))?;
    m.add_wrapped(wrap_pyfunction!(regr_syy))?;
    m.add_wrapped(wrap_pyfunction!(first_value))?;
    m.add_wrapped(wrap_pyfunction!(last_value))?;
    m.add_wrapped(wrap_pyfunction!(nth_value))?;
    m.add_wrapped(wrap_pyfunction!(bit_and))?;
    m.add_wrapped(wrap_pyfunction!(bit_or))?;
    m.add_wrapped(wrap_pyfunction!(bit_xor))?;
    m.add_wrapped(wrap_pyfunction!(bool_and))?;
    m.add_wrapped(wrap_pyfunction!(bool_or))?;

    //Binary String Functions
    m.add_wrapped(wrap_pyfunction!(encode))?;
    m.add_wrapped(wrap_pyfunction!(decode))?;

    // Array Functions
    m.add_wrapped(wrap_pyfunction!(array_append))?;
    m.add_wrapped(wrap_pyfunction!(array_concat))?;
    m.add_wrapped(wrap_pyfunction!(array_cat))?;
    m.add_wrapped(wrap_pyfunction!(array_dims))?;
    m.add_wrapped(wrap_pyfunction!(array_distinct))?;
    m.add_wrapped(wrap_pyfunction!(array_element))?;
    m.add_wrapped(wrap_pyfunction!(array_empty))?;
    m.add_wrapped(wrap_pyfunction!(array_length))?;
    m.add_wrapped(wrap_pyfunction!(array_has))?;
    m.add_wrapped(wrap_pyfunction!(array_has_all))?;
    m.add_wrapped(wrap_pyfunction!(array_has_any))?;
    m.add_wrapped(wrap_pyfunction!(array_position))?;
    m.add_wrapped(wrap_pyfunction!(array_positions))?;
    m.add_wrapped(wrap_pyfunction!(array_to_string))?;
    m.add_wrapped(wrap_pyfunction!(array_intersect))?;
    m.add_wrapped(wrap_pyfunction!(array_union))?;
    m.add_wrapped(wrap_pyfunction!(array_except))?;
    m.add_wrapped(wrap_pyfunction!(array_resize))?;
    m.add_wrapped(wrap_pyfunction!(array_ndims))?;
    m.add_wrapped(wrap_pyfunction!(array_prepend))?;
    m.add_wrapped(wrap_pyfunction!(array_pop_back))?;
    m.add_wrapped(wrap_pyfunction!(array_pop_front))?;
    m.add_wrapped(wrap_pyfunction!(array_remove))?;
    m.add_wrapped(wrap_pyfunction!(array_remove_n))?;
    m.add_wrapped(wrap_pyfunction!(array_remove_all))?;
    m.add_wrapped(wrap_pyfunction!(array_repeat))?;
    m.add_wrapped(wrap_pyfunction!(array_replace))?;
    m.add_wrapped(wrap_pyfunction!(array_replace_n))?;
    m.add_wrapped(wrap_pyfunction!(array_replace_all))?;
    m.add_wrapped(wrap_pyfunction!(array_sort))?;
    m.add_wrapped(wrap_pyfunction!(array_slice))?;
    m.add_wrapped(wrap_pyfunction!(flatten))?;
    m.add_wrapped(wrap_pyfunction!(cardinality))?;

    // Window Functions
    m.add_wrapped(wrap_pyfunction!(lead))?;
    m.add_wrapped(wrap_pyfunction!(lag))?;
    m.add_wrapped(wrap_pyfunction!(rank))?;
    m.add_wrapped(wrap_pyfunction!(row_number))?;
    m.add_wrapped(wrap_pyfunction!(dense_rank))?;
    m.add_wrapped(wrap_pyfunction!(percent_rank))?;
    m.add_wrapped(wrap_pyfunction!(cume_dist))?;
    m.add_wrapped(wrap_pyfunction!(ntile))?;

    Ok(())
}
