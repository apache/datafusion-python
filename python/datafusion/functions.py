# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""User functions for operating on :py:class:`~datafusion.expr.Expr`."""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from datafusion._internal import functions as f
from datafusion.common import NullTreatment
from datafusion.expr import (
    CaseBuilder,
    Expr,
    SortExpr,
    SortKey,
    expr_list_to_raw_expr_list,
    sort_list_to_raw_sort_list,
    sort_or_default,
    _to_raw_literal_expr,
)

__all__ = [
    "abs",
    "acos",
    "acosh",
    "alias",
    "approx_distinct",
    "approx_median",
    "approx_percentile_cont",
    "approx_percentile_cont_with_weight",
    "array",
    "array_agg",
    "array_any_value",
    "array_append",
    "array_cat",
    "array_concat",
    "array_contains",
    "array_dims",
    "array_distance",
    "array_distinct",
    "array_element",
    "array_empty",
    "array_except",
    "array_extract",
    "array_has",
    "array_has_all",
    "array_has_any",
    "array_indexof",
    "array_intersect",
    "array_join",
    "array_length",
    "array_max",
    "array_min",
    "array_ndims",
    "array_pop_back",
    "array_pop_front",
    "array_position",
    "array_positions",
    "array_prepend",
    "array_push_back",
    "array_push_front",
    "array_remove",
    "array_remove_all",
    "array_remove_n",
    "array_repeat",
    "array_replace",
    "array_replace_all",
    "array_replace_n",
    "array_resize",
    "array_reverse",
    "array_slice",
    "array_sort",
    "array_to_string",
    "array_union",
    "arrays_overlap",
    "arrays_zip",
    "arrow_cast",
    "arrow_metadata",
    "arrow_typeof",
    "ascii",
    "asin",
    "asinh",
    "atan",
    "atan2",
    "atanh",
    "avg",
    "bit_and",
    "bit_length",
    "bit_or",
    "bit_xor",
    "bool_and",
    "bool_or",
    "btrim",
    "cardinality",
    "case",
    "cbrt",
    "ceil",
    "char_length",
    "character_length",
    "chr",
    "coalesce",
    "col",
    "concat",
    "concat_ws",
    "contains",
    "corr",
    "cos",
    "cosh",
    "cot",
    "count",
    "count_star",
    "covar",
    "covar_pop",
    "covar_samp",
    "cume_dist",
    "current_date",
    "current_time",
    "current_timestamp",
    "date_bin",
    "date_format",
    "date_part",
    "date_trunc",
    "datepart",
    "datetrunc",
    "decode",
    "degrees",
    "dense_rank",
    "digest",
    "element_at",
    "empty",
    "encode",
    "ends_with",
    "exp",
    "extract",
    "factorial",
    "find_in_set",
    "first_value",
    "flatten",
    "floor",
    "from_unixtime",
    "gcd",
    "gen_series",
    "generate_series",
    "get_field",
    "greatest",
    "grouping",
    "ifnull",
    "in_list",
    "initcap",
    "isnan",
    "iszero",
    "lag",
    "last_value",
    "lcm",
    "lead",
    "least",
    "left",
    "length",
    "levenshtein",
    "list_any_value",
    "list_append",
    "list_cat",
    "list_concat",
    "list_contains",
    "list_dims",
    "list_distance",
    "list_distinct",
    "list_element",
    "list_empty",
    "list_except",
    "list_extract",
    "list_has",
    "list_has_all",
    "list_has_any",
    "list_indexof",
    "list_intersect",
    "list_join",
    "list_length",
    "list_max",
    "list_min",
    "list_ndims",
    "list_overlap",
    "list_pop_back",
    "list_pop_front",
    "list_position",
    "list_positions",
    "list_prepend",
    "list_push_back",
    "list_push_front",
    "list_remove",
    "list_remove_all",
    "list_remove_n",
    "list_repeat",
    "list_replace",
    "list_replace_all",
    "list_replace_n",
    "list_resize",
    "list_reverse",
    "list_slice",
    "list_sort",
    "list_to_string",
    "list_union",
    "list_zip",
    "ln",
    "log",
    "log2",
    "log10",
    "lower",
    "lpad",
    "ltrim",
    "make_array",
    "make_date",
    "make_list",
    "make_map",
    "make_time",
    "map_entries",
    "map_extract",
    "map_keys",
    "map_values",
    "max",
    "md5",
    "mean",
    "median",
    "min",
    "named_struct",
    "nanvl",
    "now",
    "nth_value",
    "ntile",
    "nullif",
    "nvl",
    "nvl2",
    "octet_length",
    "order_by",
    "overlay",
    "percent_rank",
    "percentile_cont",
    "pi",
    "pow",
    "power",
    "quantile_cont",
    "radians",
    "random",
    "range",
    "rank",
    "regexp_count",
    "regexp_instr",
    "regexp_like",
    "regexp_match",
    "regexp_replace",
    "regr_avgx",
    "regr_avgy",
    "regr_count",
    "regr_intercept",
    "regr_r2",
    "regr_slope",
    "regr_sxx",
    "regr_sxy",
    "regr_syy",
    "repeat",
    "replace",
    "reverse",
    "right",
    "round",
    "row",
    "row_number",
    "rpad",
    "rtrim",
    "sha224",
    "sha256",
    "sha384",
    "sha512",
    "signum",
    "sin",
    "sinh",
    "split_part",
    "sqrt",
    "starts_with",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "string_agg",
    "string_to_array",
    "string_to_list",
    "strpos",
    "struct",
    "substr",
    "substr_index",
    "substring",
    "sum",
    "tan",
    "tanh",
    "to_char",
    "to_date",
    "to_hex",
    "to_local_time",
    "to_time",
    "to_timestamp",
    "to_timestamp_micros",
    "to_timestamp_millis",
    "to_timestamp_nanos",
    "to_timestamp_seconds",
    "to_unixtime",
    "today",
    "translate",
    "trim",
    "trunc",
    "union_extract",
    "union_tag",
    "upper",
    "uuid",
    "var",
    "var_pop",
    "var_population",
    "var_samp",
    "var_sample",
    "version",
    "when",
]


def isnan(expr: Expr) -> Expr:
    """Returns true if a given number is +NaN or -NaN otherwise returns false.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.0, np.nan]})
        >>> result = df.select(dfn.functions.isnan(dfn.col("a")).alias("isnan"))
        >>> result.collect_column("isnan")[1].as_py()
        True
    """
    return Expr(f.isnan(expr.expr))


def nullif(expr1: Expr, expr2: Expr) -> Expr:
    """Returns NULL if expr1 equals expr2; otherwise it returns expr1.

    This can be used to perform the inverse operation of the COALESCE expression.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2], "b": [1, 3]})
        >>> result = df.select(
        ...     dfn.functions.nullif(dfn.col("a"), dfn.col("b")).alias("nullif"))
        >>> result.collect_column("nullif").to_pylist()
        [None, 2]
    """
    return Expr(f.nullif(expr1.expr, expr2.expr))


def encode(expr: Expr, encoding: Expr) -> Expr:
    """Encode the ``input``, using the ``encoding``. encoding can be base64 or hex.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(
        ...     dfn.functions.encode(dfn.col("a"), dfn.lit("base64")).alias("enc"))
        >>> result.collect_column("enc")[0].as_py()
        'aGVsbG8'
    """
    return Expr(f.encode(expr.expr, encoding.expr))


def decode(expr: Expr, encoding: Expr) -> Expr:
    """Decode the ``input``, using the ``encoding``. encoding can be base64 or hex.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["aGVsbG8="]})
        >>> result = df.select(
        ...     dfn.functions.decode(dfn.col("a"), dfn.lit("base64")).alias("dec"))
        >>> result.collect_column("dec")[0].as_py()
        b'hello'
    """
    return Expr(f.decode(expr.expr, encoding.expr))


def array_to_string(expr: Expr, delimiter: Expr) -> Expr:
    """Converts each element to its text representation.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]]})
        >>> result = df.select(
        ...     dfn.functions.array_to_string(dfn.col("a"), dfn.lit(",")).alias("s"))
        >>> result.collect_column("s")[0].as_py()
        '1,2,3'
    """
    return Expr(f.array_to_string(expr.expr, delimiter.expr.cast(pa.string())))


def array_join(expr: Expr, delimiter: Expr) -> Expr:
    """Converts each element to its text representation.

    See Also:
        This is an alias for :py:func:`array_to_string`.
    """
    return array_to_string(expr, delimiter)


def list_to_string(expr: Expr, delimiter: Expr) -> Expr:
    """Converts each element to its text representation.

    See Also:
        This is an alias for :py:func:`array_to_string`.
    """
    return array_to_string(expr, delimiter)


def list_join(expr: Expr, delimiter: Expr) -> Expr:
    """Converts each element to its text representation.

    See Also:
        This is an alias for :py:func:`array_to_string`.
    """
    return array_to_string(expr, delimiter)


def in_list(arg: Expr, values: list[Expr], negated: bool = False) -> Expr:
    """Returns whether the argument is contained within the list ``values``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2, 3]})
        >>> result = df.select(
        ...     dfn.functions.in_list(
        ...         dfn.col("a"), [dfn.lit(1), dfn.lit(3)]
        ...     ).alias("in")
        ... )
        >>> result.collect_column("in").to_pylist()
        [True, False, True]

        >>> result = df.select(
        ...     dfn.functions.in_list(
        ...         dfn.col("a"), [dfn.lit(1), dfn.lit(3)],
        ...         negated=True,
        ...     ).alias("not_in")
        ... )
        >>> result.collect_column("not_in").to_pylist()
        [False, True, False]
    """
    values = [v.expr for v in values]
    return Expr(f.in_list(arg.expr, values, negated))


def digest(value: Expr, method: Expr) -> Expr:
    """Computes the binary hash of an expression using the specified algorithm.

    Standard algorithms are md5, sha224, sha256, sha384, sha512, blake2s,
    blake2b, and blake3.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(
        ...     dfn.functions.digest(dfn.col("a"), dfn.lit("md5")).alias("d"))
        >>> len(result.collect_column("d")[0].as_py()) > 0
        True
    """
    return Expr(f.digest(value.expr, method.expr))


def contains(string: Expr, search_str: Expr) -> Expr:
    """Returns true if ``search_str`` is found within ``string`` (case-sensitive).

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["the quick brown fox"]})
        >>> result = df.select(
        ...     dfn.functions.contains(dfn.col("a"), dfn.lit("brown")).alias("c"))
        >>> result.collect_column("c")[0].as_py()
        True
    """
    return Expr(f.contains(string.expr, search_str.expr))


def concat(*args: Expr) -> Expr:
    """Concatenates the text representations of all the arguments.

    NULL arguments are ignored.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"], "b": [" world"]})
        >>> result = df.select(
        ...     dfn.functions.concat(dfn.col("a"), dfn.col("b")).alias("c")
        ... )
        >>> result.collect_column("c")[0].as_py()
        'hello world'
    """
    args = [arg.expr for arg in args]
    return Expr(f.concat(args))


def concat_ws(separator: str, *args: Expr) -> Expr:
    """Concatenates the list ``args`` with the separator.

    ``NULL`` arguments are ignored. ``separator`` should not be ``NULL``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"], "b": ["world"]})
        >>> result = df.select(
        ...     dfn.functions.concat_ws("-", dfn.col("a"), dfn.col("b")).alias("c"))
        >>> result.collect_column("c")[0].as_py()
        'hello-world'
    """
    args = [arg.expr for arg in args]
    return Expr(f.concat_ws(separator, args))


def order_by(expr: Expr, ascending: bool = True, nulls_first: bool = True) -> SortExpr:
    """Creates a new sort expression.

    Examples:
        >>> sort_expr = dfn.functions.order_by(
        ...     dfn.col("a"), ascending=False)
        >>> sort_expr.ascending()
        False

        >>> sort_expr = dfn.functions.order_by(
        ...     dfn.col("a"), ascending=True, nulls_first=False)
        >>> sort_expr.nulls_first()
        False
    """
    return SortExpr(expr, ascending=ascending, nulls_first=nulls_first)


def alias(expr: Expr, name: str, metadata: dict[str, str] | None = None) -> Expr:
    """Creates an alias expression with an optional metadata dictionary.

    Args:
        expr: The expression to alias
        name: The alias name
        metadata: Optional metadata to attach to the column

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2]})
        >>> result = df.select(
        ...     dfn.functions.alias(
        ...         dfn.col("a"), "b"
        ...     )
        ... )
        >>> result.collect_column("b")[0].as_py()
        1

        >>> result = df.select(
        ...     dfn.functions.alias(
        ...         dfn.col("a"), "b", metadata={"info": "test"}
        ...     )
        ... )
        >>> result.schema()
        b: int64
          -- field metadata --
          info: 'test'
    """
    return Expr(f.alias(expr.expr, name, metadata))


def col(name: str) -> Expr:
    """Creates a column reference expression.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2, 3]})
        >>> df.select(dfn.functions.col("a")).collect_column("a")[0].as_py()
        1
    """
    return Expr(f.col(name))


def count_star(filter: Expr | None = None) -> Expr:
    """Create a COUNT(1) aggregate expression.

    This aggregate function will count all of the rows in the partition.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``distinct``, and ``null_treatment``.

    Args:
        filter: If provided, only count rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2, 3]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.count_star(
        ...     ).alias("cnt")])
        >>> result.collect_column("cnt")[0].as_py()
        3

        >>> result = df.aggregate(
        ...     [], [dfn.functions.count_star(
        ...         filter=dfn.col("a") > dfn.lit(1)
        ...     ).alias("cnt")])
        >>> result.collect_column("cnt")[0].as_py()
        2
    """
    return count(Expr.literal(1), filter=filter)


def case(expr: Expr) -> CaseBuilder:
    """Create a case expression.

    Create a :py:class:`~datafusion.expr.CaseBuilder` to match cases for the
    expression ``expr``. See :py:class:`~datafusion.expr.CaseBuilder` for
    detailed usage.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2, 3]})
        >>> result = df.select(
        ...     dfn.functions.case(dfn.col("a")).when(dfn.lit(1),
        ...     dfn.lit("one")).otherwise(dfn.lit("other")).alias("c"))
        >>> result.collect_column("c")[0].as_py()
        'one'
    """
    return CaseBuilder(f.case(expr.expr))


def when(when: Expr, then: Expr) -> CaseBuilder:
    """Create a case expression that has no base expression.

    Create a :py:class:`~datafusion.expr.CaseBuilder` to match cases for the
    expression ``expr``. See :py:class:`~datafusion.expr.CaseBuilder` for
    detailed usage.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2, 3]})
        >>> result = df.select(
        ...     dfn.functions.when(dfn.col("a") > dfn.lit(2),
        ...     dfn.lit("big")).otherwise(dfn.lit("small")).alias("c"))
        >>> result.collect_column("c")[2].as_py()
        'big'
    """
    return CaseBuilder(f.when(when.expr, then.expr))


# scalar functions
def abs(arg: Expr) -> Expr:
    """Return the absolute value of a given number.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [-1, 0, 1]})
        >>> result = df.select(dfn.functions.abs(dfn.col("a")).alias("abs"))
        >>> result.collect_column("abs")[0].as_py()
        1
    """
    return Expr(f.abs(arg.expr))


def acos(arg: Expr) -> Expr:
    """Returns the arc cosine or inverse cosine of a number.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.0]})
        >>> result = df.select(dfn.functions.acos(dfn.col("a")).alias("acos"))
        >>> result.collect_column("acos")[0].as_py()
        0.0
    """
    return Expr(f.acos(arg.expr))


def acosh(arg: Expr) -> Expr:
    """Returns inverse hyperbolic cosine.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.0]})
        >>> result = df.select(dfn.functions.acosh(dfn.col("a")).alias("acosh"))
        >>> result.collect_column("acosh")[0].as_py()
        0.0
    """
    return Expr(f.acosh(arg.expr))


def ascii(arg: Expr) -> Expr:
    """Returns the numeric code of the first character of the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["a","b","c"]})
        >>> ascii_df = df.select(dfn.functions.ascii(dfn.col("a")).alias("ascii"))
        >>> ascii_df.collect_column("ascii")[0].as_py()
        97
    """
    return Expr(f.ascii(arg.expr))


def asin(arg: Expr) -> Expr:
    """Returns the arc sine or inverse sine of a number.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0.0]})
        >>> result = df.select(dfn.functions.asin(dfn.col("a")).alias("asin"))
        >>> result.collect_column("asin")[0].as_py()
        0.0
    """
    return Expr(f.asin(arg.expr))


def asinh(arg: Expr) -> Expr:
    """Returns inverse hyperbolic sine.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0.0]})
        >>> result = df.select(dfn.functions.asinh(dfn.col("a")).alias("asinh"))
        >>> result.collect_column("asinh")[0].as_py()
        0.0
    """
    return Expr(f.asinh(arg.expr))


def atan(arg: Expr) -> Expr:
    """Returns inverse tangent of a number.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0.0]})
        >>> result = df.select(dfn.functions.atan(dfn.col("a")).alias("atan"))
        >>> result.collect_column("atan")[0].as_py()
        0.0
    """
    return Expr(f.atan(arg.expr))


def atanh(arg: Expr) -> Expr:
    """Returns inverse hyperbolic tangent.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0.0]})
        >>> result = df.select(dfn.functions.atanh(dfn.col("a")).alias("atanh"))
        >>> result.collect_column("atanh")[0].as_py()
        0.0
    """
    return Expr(f.atanh(arg.expr))


def atan2(y: Expr, x: Expr) -> Expr:
    """Returns inverse tangent of a division given in the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"y": [0.0], "x": [1.0]})
        >>> result = df.select(
        ...     dfn.functions.atan2(dfn.col("y"), dfn.col("x")).alias("atan2"))
        >>> result.collect_column("atan2")[0].as_py()
        0.0
    """
    return Expr(f.atan2(y.expr, x.expr))


def bit_length(arg: Expr) -> Expr:
    """Returns the number of bits in the string argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["a","b","c"]})
        >>> bit_df = df.select(dfn.functions.bit_length(dfn.col("a")).alias("bit_len"))
        >>> bit_df.collect_column("bit_len")[0].as_py()
        8
    """
    return Expr(f.bit_length(arg.expr))


def btrim(arg: Expr) -> Expr:
    """Removes all characters, spaces by default, from both sides of a string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [" a  "]})
        >>> trim_df = df.select(dfn.functions.btrim(dfn.col("a")).alias("trimmed"))
        >>> trim_df.collect_column("trimmed")[0].as_py()
        'a'
    """
    return Expr(f.btrim(arg.expr))


def cbrt(arg: Expr) -> Expr:
    """Returns the cube root of a number.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [27]})
        >>> cbrt_df = df.select(dfn.functions.cbrt(dfn.col("a")).alias("cbrt"))
        >>> cbrt_df.collect_column("cbrt")[0].as_py()
        3.0
    """
    return Expr(f.cbrt(arg.expr))


def ceil(arg: Expr) -> Expr:
    """Returns the nearest integer greater than or equal to argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.9]})
        >>> ceil_df = df.select(dfn.functions.ceil(dfn.col("a")).alias("ceil"))
        >>> ceil_df.collect_column("ceil")[0].as_py()
        2.0
    """
    return Expr(f.ceil(arg.expr))


def character_length(arg: Expr) -> Expr:
    """Returns the number of characters in the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["abc","b","c"]})
        >>> char_len_df = df.select(
        ...     dfn.functions.character_length(dfn.col("a")).alias("char_len"))
        >>> char_len_df.collect_column("char_len")[0].as_py()
        3
    """
    return Expr(f.character_length(arg.expr))


def length(string: Expr) -> Expr:
    """The number of characters in the ``string``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(dfn.functions.length(dfn.col("a")).alias("len"))
        >>> result.collect_column("len")[0].as_py()
        5
    """
    return Expr(f.length(string.expr))


def char_length(string: Expr) -> Expr:
    """The number of characters in the ``string``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(dfn.functions.char_length(dfn.col("a")).alias("len"))
        >>> result.collect_column("len")[0].as_py()
        5
    """
    return Expr(f.char_length(string.expr))


def chr(arg: Expr) -> Expr:
    """Converts the Unicode code point to a UTF8 character.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [65]})
        >>> result = df.select(dfn.functions.chr(dfn.col("a")).alias("chr"))
        >>> result.collect_column("chr")[0].as_py()
        'A'
    """
    return Expr(f.chr(arg.expr))


def coalesce(*args: Expr) -> Expr:
    """Returns the value of the first expr in ``args`` which is not NULL.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [None, 1], "b": [2, 3]})
        >>> result = df.select(
        ...     dfn.functions.coalesce(dfn.col("a"), dfn.col("b")).alias("c"))
        >>> result.collect_column("c")[0].as_py()
        2
    """
    args = [arg.expr for arg in args]
    return Expr(f.coalesce(*args))


def cos(arg: Expr) -> Expr:
    """Returns the cosine of the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0,-1,1]})
        >>> cos_df = df.select(dfn.functions.cos(dfn.col("a")).alias("cos"))
        >>> cos_df.collect_column("cos")[0].as_py()
        1.0
    """
    return Expr(f.cos(arg.expr))


def cosh(arg: Expr) -> Expr:
    """Returns the hyperbolic cosine of the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0,-1,1]})
        >>> cosh_df = df.select(dfn.functions.cosh(dfn.col("a")).alias("cosh"))
        >>> cosh_df.collect_column("cosh")[0].as_py()
        1.0
    """
    return Expr(f.cosh(arg.expr))


def cot(arg: Expr) -> Expr:
    """Returns the cotangent of the argument.

    Examples:
        >>> from math import pi
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [pi / 4]})
        >>> result = df.select(
        ...     dfn.functions.cot(dfn.col("a")).alias("cot")
        ... )
        >>> result.collect_column("cot")[0].as_py()
        1.0...
    """
    return Expr(f.cot(arg.expr))


def degrees(arg: Expr) -> Expr:
    """Converts the argument from radians to degrees.

    Examples:
        >>> from math import pi
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0,pi,2*pi]})
        >>> deg_df = df.select(dfn.functions.degrees(dfn.col("a")).alias("deg"))
        >>> deg_df.collect_column("deg")[2].as_py()
        360.0
    """
    return Expr(f.degrees(arg.expr))


def ends_with(arg: Expr, suffix: Expr) -> Expr:
    """Returns true if the ``string`` ends with the ``suffix``, false otherwise.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["abc","b","c"]})
        >>> ends_with_df = df.select(
        ...     dfn.functions.ends_with(dfn.col("a"), dfn.lit("c")).alias("ends_with"))
        >>> ends_with_df.collect_column("ends_with")[0].as_py()
        True
    """
    return Expr(f.ends_with(arg.expr, suffix.expr))


def exp(arg: Expr) -> Expr:
    """Returns the exponential of the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0.0]})
        >>> result = df.select(dfn.functions.exp(dfn.col("a")).alias("exp"))
        >>> result.collect_column("exp")[0].as_py()
        1.0
    """
    return Expr(f.exp(arg.expr))


def factorial(arg: Expr) -> Expr:
    """Returns the factorial of the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [3]})
        >>> result = df.select(
        ...     dfn.functions.factorial(dfn.col("a")).alias("factorial")
        ... )
        >>> result.collect_column("factorial")[0].as_py()
        6
    """
    return Expr(f.factorial(arg.expr))


def find_in_set(string: Expr, string_list: Expr) -> Expr:
    """Find a string in a list of strings.

    Returns a value in the range of 1 to N if the string is in the string list
    ``string_list`` consisting of N substrings.

    The string list is a string composed of substrings separated by ``,`` characters.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["b"]})
        >>> result = df.select(
        ...     dfn.functions.find_in_set(dfn.col("a"), dfn.lit("a,b,c")).alias("pos"))
        >>> result.collect_column("pos")[0].as_py()
        2
    """
    return Expr(f.find_in_set(string.expr, string_list.expr))


def floor(arg: Expr) -> Expr:
    """Returns the nearest integer less than or equal to the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.9]})
        >>> floor_df = df.select(dfn.functions.floor(dfn.col("a")).alias("floor"))
        >>> floor_df.collect_column("floor")[0].as_py()
        1.0
    """
    return Expr(f.floor(arg.expr))


def gcd(x: Expr, y: Expr) -> Expr:
    """Returns the greatest common divisor.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [12], "b": [8]})
        >>> result = df.select(
        ...     dfn.functions.gcd(dfn.col("a"), dfn.col("b")).alias("gcd")
        ... )
        >>> result.collect_column("gcd")[0].as_py()
        4
    """
    return Expr(f.gcd(x.expr, y.expr))


def greatest(*args: Expr) -> Expr:
    """Returns the greatest value from a list of expressions.

    Returns NULL if all expressions are NULL.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 3], "b": [2, 1]})
        >>> result = df.select(
        ...     dfn.functions.greatest(dfn.col("a"), dfn.col("b")).alias("greatest"))
        >>> result.collect_column("greatest")[0].as_py()
        2
        >>> result.collect_column("greatest")[1].as_py()
        3
    """
    exprs = [arg.expr for arg in args]
    return Expr(f.greatest(*exprs))


def ifnull(x: Expr, y: Expr) -> Expr:
    """Returns ``x`` if ``x`` is not NULL. Otherwise returns ``y``.

    See Also:
        This is an alias for :py:func:`nvl`.
    """
    return nvl(x, y)


def initcap(string: Expr) -> Expr:
    """Set the initial letter of each word to capital.

    Converts the first letter of each word in ``string`` to uppercase and the remaining
    characters to lowercase.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["the cat"]})
        >>> cap_df = df.select(dfn.functions.initcap(dfn.col("a")).alias("cap"))
        >>> cap_df.collect_column("cap")[0].as_py()
        'The Cat'
    """
    return Expr(f.initcap(string.expr))


def instr(string: Expr, substring: Expr) -> Expr:
    """Finds the position from where the ``substring`` matches the ``string``.

    See Also:
        This is an alias for :py:func:`strpos`.
    """
    return strpos(string, substring)


def iszero(arg: Expr) -> Expr:
    """Returns true if a given number is +0.0 or -0.0 otherwise returns false.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0.0, 1.0]})
        >>> result = df.select(dfn.functions.iszero(dfn.col("a")).alias("iz"))
        >>> result.collect_column("iz")[0].as_py()
        True
    """
    return Expr(f.iszero(arg.expr))


def lcm(x: Expr, y: Expr) -> Expr:
    """Returns the least common multiple.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [4], "b": [6]})
        >>> result = df.select(
        ...     dfn.functions.lcm(dfn.col("a"), dfn.col("b")).alias("lcm")
        ... )
        >>> result.collect_column("lcm")[0].as_py()
        12
    """
    return Expr(f.lcm(x.expr, y.expr))


def least(*args: Expr) -> Expr:
    """Returns the least value from a list of expressions.

    Returns NULL if all expressions are NULL.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 3], "b": [2, 1]})
        >>> result = df.select(
        ...     dfn.functions.least(dfn.col("a"), dfn.col("b")).alias("least"))
        >>> result.collect_column("least")[0].as_py()
        1
        >>> result.collect_column("least")[1].as_py()
        1
    """
    exprs = [arg.expr for arg in args]
    return Expr(f.least(*exprs))


def left(string: Expr, n: Expr) -> Expr:
    """Returns the first ``n`` characters in the ``string``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["the cat"]})
        >>> left_df = df.select(
        ...     dfn.functions.left(dfn.col("a"), dfn.lit(3)).alias("left"))
        >>> left_df.collect_column("left")[0].as_py()
        'the'
    """
    return Expr(f.left(string.expr, n.expr))


def levenshtein(string1: Expr, string2: Expr) -> Expr:
    """Returns the Levenshtein distance between the two given strings.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["kitten"]})
        >>> result = df.select(
        ...     dfn.functions.levenshtein(dfn.col("a"), dfn.lit("sitting")).alias("d"))
        >>> result.collect_column("d")[0].as_py()
        3
    """
    return Expr(f.levenshtein(string1.expr, string2.expr))


def ln(arg: Expr) -> Expr:
    """Returns the natural logarithm (base e) of the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.0]})
        >>> result = df.select(dfn.functions.ln(dfn.col("a")).alias("ln"))
        >>> result.collect_column("ln")[0].as_py()
        0.0
    """
    return Expr(f.ln(arg.expr))


def log(base: Expr, num: Expr) -> Expr:
    """Returns the logarithm of a number for a particular ``base``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [100.0]})
        >>> result = df.select(
        ...     dfn.functions.log(dfn.lit(10.0), dfn.col("a")).alias("log")
        ... )
        >>> result.collect_column("log")[0].as_py()
        2.0
    """
    return Expr(f.log(base.expr, num.expr))


def log10(arg: Expr) -> Expr:
    """Base 10 logarithm of the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [100.0]})
        >>> result = df.select(dfn.functions.log10(dfn.col("a")).alias("log10"))
        >>> result.collect_column("log10")[0].as_py()
        2.0
    """
    return Expr(f.log10(arg.expr))


def log2(arg: Expr) -> Expr:
    """Base 2 logarithm of the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [8.0]})
        >>> result = df.select(dfn.functions.log2(dfn.col("a")).alias("log2"))
        >>> result.collect_column("log2")[0].as_py()
        3.0
    """
    return Expr(f.log2(arg.expr))


def lower(arg: Expr) -> Expr:
    """Converts a string to lowercase.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["THE CaT"]})
        >>> lower_df = df.select(dfn.functions.lower(dfn.col("a")).alias("lower"))
        >>> lower_df.collect_column("lower")[0].as_py()
        'the cat'
    """
    return Expr(f.lower(arg.expr))


def lpad(string: Expr, count: Expr, characters: Expr | None = None) -> Expr:
    """Add left padding to a string.

    Extends the string to length length by prepending the characters fill (a
    space by default). If the string is already longer than length then it is
    truncated (on the right).

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["the cat", "a hat"]})
        >>> lpad_df = df.select(
        ...     dfn.functions.lpad(
        ...         dfn.col("a"), dfn.lit(6)
        ...     ).alias("lpad"))
        >>> lpad_df.collect_column("lpad")[0].as_py()
        'the ca'
        >>> lpad_df.collect_column("lpad")[1].as_py()
        ' a hat'

        >>> result = df.select(
        ...     dfn.functions.lpad(
        ...         dfn.col("a"), dfn.lit(10), characters=dfn.lit(".")
        ...     ).alias("lpad"))
        >>> result.collect_column("lpad")[0].as_py()
        '...the cat'
    """
    characters = characters if characters is not None else Expr.literal(" ")
    return Expr(f.lpad(string.expr, count.expr, characters.expr))


def ltrim(arg: Expr) -> Expr:
    """Removes all characters, spaces by default, from the beginning of a string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [" a  "]})
        >>> trim_df = df.select(dfn.functions.ltrim(dfn.col("a")).alias("trimmed"))
        >>> trim_df.collect_column("trimmed")[0].as_py()
        'a  '
    """
    return Expr(f.ltrim(arg.expr))


def md5(arg: Expr) -> Expr:
    """Computes an MD5 128-bit checksum for a string expression.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(dfn.functions.md5(dfn.col("a")).alias("md5"))
        >>> result.collect_column("md5")[0].as_py()
        '5d41402abc4b2a76b9719d911017c592'
    """
    return Expr(f.md5(arg.expr))


def nanvl(x: Expr, y: Expr) -> Expr:
    """Returns ``x`` if ``x`` is not ``NaN``. Otherwise returns ``y``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [np.nan, 1.0], "b": [0.0, 0.0]})
        >>> nanvl_df = df.select(
        ...     dfn.functions.nanvl(dfn.col("a"), dfn.col("b")).alias("nanvl"))
        >>> nanvl_df.collect_column("nanvl")[0].as_py()
        0.0
        >>> nanvl_df.collect_column("nanvl")[1].as_py()
        1.0
    """
    return Expr(f.nanvl(x.expr, y.expr))


def nvl(x: Expr, y: Expr) -> Expr:
    """Returns ``x`` if ``x`` is not ``NULL``. Otherwise returns ``y``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [None, 1], "b": [0, 0]})
        >>> nvl_df = df.select(
        ...     dfn.functions.nvl(dfn.col("a"), dfn.col("b")).alias("nvl")
        ... )
        >>> nvl_df.collect_column("nvl")[0].as_py()
        0
        >>> nvl_df.collect_column("nvl")[1].as_py()
        1
    """
    return Expr(f.nvl(x.expr, y.expr))


def nvl2(x: Expr, y: Expr, z: Expr) -> Expr:
    """Returns ``y`` if ``x`` is not NULL. Otherwise returns ``z``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [None, 1], "b": [10, 20], "c": [30, 40]})
        >>> result = df.select(
        ...     dfn.functions.nvl2(
        ...         dfn.col("a"), dfn.col("b"), dfn.col("c")).alias("nvl2")
        ... )
        >>> result.collect_column("nvl2")[0].as_py()
        30
        >>> result.collect_column("nvl2")[1].as_py()
        20
    """
    return Expr(f.nvl2(x.expr, y.expr, z.expr))


def octet_length(arg: Expr) -> Expr:
    """Returns the number of bytes of a string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(dfn.functions.octet_length(dfn.col("a")).alias("len"))
        >>> result.collect_column("len")[0].as_py()
        5
    """
    return Expr(f.octet_length(arg.expr))


def overlay(
    string: Expr, substring: Expr, start: Expr, length: Expr | None = None
) -> Expr:
    """Replace a substring with a new substring.

    Replace the substring of string that starts at the ``start``'th character and
    extends for ``length`` characters with new substring.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["abcdef"]})
        >>> result = df.select(
        ...     dfn.functions.overlay(dfn.col("a"), dfn.lit("XY"), dfn.lit(3),
        ...     dfn.lit(2)).alias("o"))
        >>> result.collect_column("o")[0].as_py()
        'abXYef'
    """
    if length is None:
        return Expr(f.overlay(string.expr, substring.expr, start.expr))
    return Expr(f.overlay(string.expr, substring.expr, start.expr, length.expr))


def pi() -> Expr:
    """Returns an approximate value of π.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> from math import pi
        >>> result = df.select(
        ...     dfn.functions.pi().alias("pi")
        ... )
        >>> result.collect_column("pi")[0].as_py() == pi
        True
    """
    return Expr(f.pi())


def position(string: Expr, substring: Expr) -> Expr:
    """Finds the position from where the ``substring`` matches the ``string``.

    See Also:
        This is an alias for :py:func:`strpos`.
    """
    return strpos(string, substring)


def power(base: Expr, exponent: Expr) -> Expr:
    """Returns ``base`` raised to the power of ``exponent``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [2.0]})
        >>> result = df.select(
        ...     dfn.functions.power(dfn.col("a"), dfn.lit(3.0)).alias("pow")
        ... )
        >>> result.collect_column("pow")[0].as_py()
        8.0
    """
    return Expr(f.power(base.expr, exponent.expr))


def pow(base: Expr, exponent: Expr) -> Expr:
    """Returns ``base`` raised to the power of ``exponent``.

    See Also:
        This is an alias of :py:func:`power`.
    """
    return power(base, exponent)


def radians(arg: Expr) -> Expr:
    """Converts the argument from degrees to radians.

    Examples:
        >>> from math import pi
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [180.0]})
        >>> result = df.select(
        ...     dfn.functions.radians(dfn.col("a")).alias("rad")
        ... )
        >>> result.collect_column("rad")[0].as_py() == pi
        True
    """
    return Expr(f.radians(arg.expr))


def regexp_like(
    string: Expr, regex: Expr | Any, flags: Expr | Any | None = None
) -> Expr:
    r"""Find if any regular expression (regex) matches exist.

    Tests a string using a regular expression returning true if at least one match,
    false otherwise.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello123"]})
        >>> result = df.select(
        ...     dfn.functions.regexp_like(
        ...         dfn.col("a"), dfn.lit("\\d+")
        ...     ).alias("m")
        ... )
        >>> result.collect_column("m")[0].as_py()
        True

        Use ``flags`` for case-insensitive matching:

        >>> result = df.select(
        ...     dfn.functions.regexp_like(
        ...         dfn.col("a"), dfn.lit("HELLO"),
        ...         flags=dfn.lit("i"),
        ...     ).alias("m")
        ... )
        >>> result.collect_column("m")[0].as_py()
        True
    """
    # if flags is not None:
    #     flags = flags.expr
    # return Expr(f.regexp_like(string.expr, regex.expr, flags))
    flags = _to_raw_literal_expr(flags) if flags is not None else None
    return Expr(f.regexp_like(string.expr, _to_raw_literal_expr(regex), flags))


def regexp_match(
    string: Expr, regex: Expr | Any, flags: Expr | Any | None = None
) -> Expr:
    r"""Perform regular expression (regex) matching.

    Returns an array with each element containing the leftmost-first match of the
    corresponding index in ``regex`` to string in ``string``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello 42 world"]})
        >>> result = df.select(
        ...     dfn.functions.regexp_match(
        ...         dfn.col("a"), dfn.lit("(\\d+)")
        ...     ).alias("m")
        ... )
        >>> result.collect_column("m")[0].as_py()
        ['42']

        Use ``flags`` for case-insensitive matching:

        >>> result = df.select(
        ...     dfn.functions.regexp_match(
        ...         dfn.col("a"), dfn.lit("(HELLO)"),
        ...         flags=dfn.lit("i"),
        ...     ).alias("m")
        ... )
        >>> result.collect_column("m")[0].as_py()
        ['hello']
    """
    # if flags is not None:
    #     flags = flags.expr
    # return Expr(f.regexp_match(string.expr, regex.expr, flags))
    flags = _to_raw_literal_expr(flags) if flags is not None else None
    return Expr(f.regexp_match(string.expr, _to_raw_literal_expr(regex), flags))


def regexp_replace(
    string: Expr,
    pattern: Expr | Any,
    replacement: Expr | Any,
    flags: Expr | Any | None = None,
) -> Expr:
    r"""Replaces substring(s) matching a PCRE-like regular expression.

    The full list of supported features and syntax can be found at
    <https://docs.rs/regex/latest/regex/#syntax>

    Supported flags with the addition of 'g' can be found at
    <https://docs.rs/regex/latest/regex/#grouping-and-flags>

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello 42"]})
        >>> result = df.select(
        ...     dfn.functions.regexp_replace(
        ...         dfn.col("a"), dfn.lit("\\d+"),
        ...         dfn.lit("XX")
        ...     ).alias("r")
        ... )
        >>> result.collect_column("r")[0].as_py()
        'hello XX'

        Use the ``g`` flag to replace all occurrences:

        >>> df = ctx.from_pydict({"a": ["a1 b2 c3"]})
        >>> result = df.select(
        ...     dfn.functions.regexp_replace(
        ...         dfn.col("a"), dfn.lit("\\d+"),
        ...         dfn.lit("X"), flags=dfn.lit("g"),
        ...     ).alias("r")
        ... )
        >>> result.collect_column("r")[0].as_py()
        'aX bX cX'
    """
    # if flags is not None:
    #     flags = flags.expr
    # return Expr(f.regexp_replace(string.expr, pattern.expr, replacement.expr, flags))
    flags = _to_raw_literal_expr(flags) if flags is not None else None
    pattern = _to_raw_literal_expr(pattern)
    replacement = _to_raw_literal_expr(replacement)
    return Expr(f.regexp_replace(string.expr, pattern, replacement, flags))


def regexp_count(
    string: Expr,
    pattern: Expr | Any,
    start: Expr | Any | None = None,
    flags: Expr | Any | None = None,
) -> Expr:
    """Returns the number of matches in a string.

    Optional start position (the first position is 1) to search for the regular
    expression.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["abcabc"]})
        >>> result = df.select(
        ...     dfn.functions.regexp_count(
        ...         dfn.col("a"), dfn.lit("abc")
        ...     ).alias("c"))
        >>> result.collect_column("c")[0].as_py()
        2

        Use ``start`` to begin searching from a position, and
        ``flags`` for case-insensitive matching:

        >>> result = df.select(
        ...     dfn.functions.regexp_count(
        ...         dfn.col("a"), dfn.lit("ABC"),
        ...         start=dfn.lit(4), flags=dfn.lit("i"),
        ...     ).alias("c"))
        >>> result.collect_column("c")[0].as_py()
        1
    """
    # if flags is not None:
    #     flags = flags.expr
    # start = start.expr if start is not None else start
    # return Expr(f.regexp_count(string.expr, pattern.expr, start, flags))
    flags = _to_raw_literal_expr(flags) if flags is not None else None
    start = _to_raw_literal_expr(start) if start is not None else None
    return Expr(
        f.regexp_count(string.expr, _to_raw_literal_expr(pattern), start, flags)
    )


def regexp_instr(
    values: Expr,
    regex: Expr | Any,
    start: Expr | Any | None = None,
    n: Expr | Any | None = None,
    flags: Expr | Any | None = None,
    sub_expr: Expr | Any | None = None,
) -> Expr:
    r"""Returns the position of a regular expression match in a string.

    Args:
        values: Data to search for the regular expression match.
        regex: Regular expression to search for.
        start: Optional position to start the search (the first position is 1).
        n: Optional occurrence of the match to find (the first occurrence is 1).
        flags: Optional regular expression flags to control regex behavior.
        sub_expr: Optionally capture group position instead of the entire match.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello 42 world"]})
        >>> result = df.select(
        ...     dfn.functions.regexp_instr(
        ...         dfn.col("a"), dfn.lit("\\d+")
        ...     ).alias("pos")
        ... )
        >>> result.collect_column("pos")[0].as_py()
        7

        Use ``start`` to search from a position, ``n`` for the
        nth occurrence, and ``flags`` for case-insensitive mode:

        >>> df = ctx.from_pydict({"a": ["abc ABC abc"]})
        >>> result = df.select(
        ...     dfn.functions.regexp_instr(
        ...         dfn.col("a"), dfn.lit("abc"),
        ...         start=dfn.lit(2), n=dfn.lit(1),
        ...         flags=dfn.lit("i"),
        ...     ).alias("pos")
        ... )
        >>> result.collect_column("pos")[0].as_py()
        5

        Use ``sub_expr`` to get the position of a capture group:

        >>> result = df.select(
        ...     dfn.functions.regexp_instr(
        ...         dfn.col("a"), dfn.lit("(abc)"),
        ...         sub_expr=dfn.lit(1),
        ...     ).alias("pos")
        ... )
        >>> result.collect_column("pos")[0].as_py()
        1
    """
    # start = start.expr if start is not None else None
    # n = n.expr if n is not None else None
    # flags = flags.expr if flags is not None else None
    # sub_expr = sub_expr.expr if sub_expr is not None else None
    regex = _to_raw_literal_expr(regex)
    start = _to_raw_literal_expr(start) if start is not None else None
    n = _to_raw_literal_expr(n) if n is not None else None
    flags = _to_raw_literal_expr(flags) if flags is not None else None
    sub_expr = _to_raw_literal_expr(sub_expr) if sub_expr is not None else None

    return Expr(
        f.regexp_instr(
            values.expr,
            regex,
            start,
            n,
            flags,
            sub_expr,
        )
    )


def repeat(string: Expr, n: Expr) -> Expr:
    """Repeats the ``string`` to ``n`` times.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["ha"]})
        >>> result = df.select(
        ...     dfn.functions.repeat(dfn.col("a"), dfn.lit(3)).alias("r"))
        >>> result.collect_column("r")[0].as_py()
        'hahaha'
    """
    return Expr(f.repeat(string.expr, n.expr))


def replace(string: Expr, from_val: Expr, to_val: Expr) -> Expr:
    """Replaces all occurrences of ``from_val`` with ``to_val`` in the ``string``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello world"]})
        >>> result = df.select(
        ...     dfn.functions.replace(dfn.col("a"), dfn.lit("world"),
        ...     dfn.lit("there")).alias("r"))
        >>> result.collect_column("r")[0].as_py()
        'hello there'
    """
    return Expr(f.replace(string.expr, from_val.expr, to_val.expr))


def reverse(arg: Expr) -> Expr:
    """Reverse the string argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(dfn.functions.reverse(dfn.col("a")).alias("r"))
        >>> result.collect_column("r")[0].as_py()
        'olleh'
    """
    return Expr(f.reverse(arg.expr))


def right(string: Expr, n: Expr) -> Expr:
    """Returns the last ``n`` characters in the ``string``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(dfn.functions.right(dfn.col("a"), dfn.lit(3)).alias("r"))
        >>> result.collect_column("r")[0].as_py()
        'llo'
    """
    return Expr(f.right(string.expr, n.expr))


def round(value: Expr, decimal_places: Expr | None = None) -> Expr:
    """Round the argument to the nearest integer.

    If the optional ``decimal_places`` is specified, round to the nearest number of
    decimal places. You can specify a negative number of decimal places. For example
    ``round(lit(125.2345), lit(-2))`` would yield a value of ``100.0``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.567]})
        >>> result = df.select(dfn.functions.round(dfn.col("a"), dfn.lit(2)).alias("r"))
        >>> result.collect_column("r")[0].as_py()
        1.57
    """
    if decimal_places is None:
        decimal_places = Expr.literal(0)
    return Expr(f.round(value.expr, decimal_places.expr))


def rpad(string: Expr, count: Expr, characters: Expr | None = None) -> Expr:
    """Add right padding to a string.

    Extends the string to length length by appending the characters fill (a space
    by default). If the string is already longer than length then it is truncated.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hi"]})
        >>> result = df.select(
        ...     dfn.functions.rpad(dfn.col("a"), dfn.lit(5), dfn.lit("!")).alias("r"))
        >>> result.collect_column("r")[0].as_py()
        'hi!!!'
    """
    characters = characters if characters is not None else Expr.literal(" ")
    return Expr(f.rpad(string.expr, count.expr, characters.expr))


def rtrim(arg: Expr) -> Expr:
    """Removes all characters, spaces by default, from the end of a string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [" a  "]})
        >>> trim_df = df.select(dfn.functions.rtrim(dfn.col("a")).alias("trimmed"))
        >>> trim_df.collect_column("trimmed")[0].as_py()
        ' a'
    """
    return Expr(f.rtrim(arg.expr))


def sha224(arg: Expr) -> Expr:
    """Computes the SHA-224 hash of a binary string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(
        ...     dfn.functions.sha224(dfn.col("a")).alias("h")
        ... )
        >>> result.collect_column("h")[0].as_py().hex()
        'ea09ae9cc6768c50fcee903ed054556e5bfc8347907f12598aa24193'
    """
    return Expr(f.sha224(arg.expr))


def sha256(arg: Expr) -> Expr:
    """Computes the SHA-256 hash of a binary string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(
        ...     dfn.functions.sha256(dfn.col("a")).alias("h")
        ... )
        >>> result.collect_column("h")[0].as_py().hex()
        '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'
    """
    return Expr(f.sha256(arg.expr))


def sha384(arg: Expr) -> Expr:
    """Computes the SHA-384 hash of a binary string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(
        ...     dfn.functions.sha384(dfn.col("a")).alias("h")
        ... )
        >>> result.collect_column("h")[0].as_py().hex()
        '59e1748777448c69de6b800d7a33bbfb9ff1b...
    """
    return Expr(f.sha384(arg.expr))


def sha512(arg: Expr) -> Expr:
    """Computes the SHA-512 hash of a binary string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(
        ...     dfn.functions.sha512(dfn.col("a")).alias("h")
        ... )
        >>> result.collect_column("h")[0].as_py().hex()
        '9b71d224bd62f3785d96d46ad3ea3d73319bfb...
    """
    return Expr(f.sha512(arg.expr))


def signum(arg: Expr) -> Expr:
    """Returns the sign of the argument (-1, 0, +1).

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [-5.0, 0.0, 5.0]})
        >>> result = df.select(dfn.functions.signum(dfn.col("a")).alias("s"))
        >>> result.collect_column("s").to_pylist()
        [-1.0, 0.0, 1.0]
    """
    return Expr(f.signum(arg.expr))


def sin(arg: Expr) -> Expr:
    """Returns the sine of the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0.0]})
        >>> result = df.select(dfn.functions.sin(dfn.col("a")).alias("sin"))
        >>> result.collect_column("sin")[0].as_py()
        0.0
    """
    return Expr(f.sin(arg.expr))


def sinh(arg: Expr) -> Expr:
    """Returns the hyperbolic sine of the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0.0]})
        >>> result = df.select(dfn.functions.sinh(dfn.col("a")).alias("sinh"))
        >>> result.collect_column("sinh")[0].as_py()
        0.0
    """
    return Expr(f.sinh(arg.expr))


def split_part(string: Expr, delimiter: Expr, index: Expr) -> Expr:
    """Split a string and return one part.

    Splits a string based on a delimiter and picks out the desired field based
    on the index.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["a,b,c"]})
        >>> result = df.select(
        ...     dfn.functions.split_part(
        ...         dfn.col("a"), dfn.lit(","), dfn.lit(2)
        ...     ).alias("s"))
        >>> result.collect_column("s")[0].as_py()
        'b'
    """
    return Expr(f.split_part(string.expr, delimiter.expr, index.expr))


def sqrt(arg: Expr) -> Expr:
    """Returns the square root of the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [9.0]})
        >>> result = df.select(dfn.functions.sqrt(dfn.col("a")).alias("sqrt"))
        >>> result.collect_column("sqrt")[0].as_py()
        3.0
    """
    return Expr(f.sqrt(arg.expr))


def starts_with(string: Expr, prefix: Expr) -> Expr:
    """Returns true if string starts with prefix.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello_from_datafusion"]})
        >>> result = df.select(
        ...     dfn.functions.starts_with(dfn.col("a"), dfn.lit("hello")).alias("sw"))
        >>> result.collect_column("sw")[0].as_py()
        True
    """
    return Expr(f.starts_with(string.expr, prefix.expr))


def strpos(string: Expr, substring: Expr) -> Expr:
    """Finds the position from where the ``substring`` matches the ``string``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(
        ...     dfn.functions.strpos(dfn.col("a"), dfn.lit("llo")).alias("pos"))
        >>> result.collect_column("pos")[0].as_py()
        3
    """
    return Expr(f.strpos(string.expr, substring.expr))


def substr(string: Expr, position: Expr) -> Expr:
    """Substring from the ``position`` to the end.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(
        ...     dfn.functions.substr(dfn.col("a"), dfn.lit(3)).alias("s"))
        >>> result.collect_column("s")[0].as_py()
        'llo'
    """
    return Expr(f.substr(string.expr, position.expr))


def substr_index(string: Expr, delimiter: Expr, count: Expr) -> Expr:
    """Returns an indexed substring.

    The return will be the ``string`` from before ``count`` occurrences of
    ``delimiter``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["a.b.c"]})
        >>> result = df.select(
        ...     dfn.functions.substr_index(dfn.col("a"), dfn.lit("."),
        ...     dfn.lit(2)).alias("s"))
        >>> result.collect_column("s")[0].as_py()
        'a.b'
    """
    return Expr(f.substr_index(string.expr, delimiter.expr, count.expr))


def substring(string: Expr, position: Expr, length: Expr) -> Expr:
    """Substring from the ``position`` with ``length`` characters.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello world"]})
        >>> result = df.select(
        ...     dfn.functions.substring(
        ...         dfn.col("a"), dfn.lit(1), dfn.lit(5)
        ...     ).alias("s"))
        >>> result.collect_column("s")[0].as_py()
        'hello'
    """
    return Expr(f.substring(string.expr, position.expr, length.expr))


def tan(arg: Expr) -> Expr:
    """Returns the tangent of the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0.0]})
        >>> result = df.select(dfn.functions.tan(dfn.col("a")).alias("tan"))
        >>> result.collect_column("tan")[0].as_py()
        0.0
    """
    return Expr(f.tan(arg.expr))


def tanh(arg: Expr) -> Expr:
    """Returns the hyperbolic tangent of the argument.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0.0]})
        >>> result = df.select(dfn.functions.tanh(dfn.col("a")).alias("tanh"))
        >>> result.collect_column("tanh")[0].as_py()
        0.0
    """
    return Expr(f.tanh(arg.expr))


def to_hex(arg: Expr) -> Expr:
    """Converts an integer to a hexadecimal string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [255]})
        >>> result = df.select(dfn.functions.to_hex(dfn.col("a")).alias("hex"))
        >>> result.collect_column("hex")[0].as_py()
        'ff'
    """
    return Expr(f.to_hex(arg.expr))


def now() -> Expr:
    """Returns the current timestamp in nanoseconds.

    This will use the same value for all instances of now() in same statement.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> result = df.select(
        ...     dfn.functions.now().alias("now")
        ... )

        Use .value instead of .as_py() because nanosecond timestamps
        require pandas to convert to Python datetime objects.

        >>> result.collect_column("now")[0].value > 0
        True
    """
    return Expr(f.now())


def current_timestamp() -> Expr:
    """Returns the current timestamp in nanoseconds.

    See Also:
        This is an alias for :py:func:`now`.
    """
    return now()


def to_char(arg: Expr, formatter: Expr) -> Expr:
    """Returns a string representation of a date, time, timestamp or duration.

    For usage of ``formatter`` see the rust chrono package ``strftime`` package.

    [Documentation here.](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["2021-01-01T00:00:00"]})
        >>> result = df.select(
        ...     dfn.functions.to_char(
        ...         dfn.functions.to_timestamp(dfn.col("a")),
        ...         dfn.lit("%Y/%m/%d"),
        ...     ).alias("formatted")
        ... )
        >>> result.collect_column("formatted")[0].as_py()
        '2021/01/01'
    """
    return Expr(f.to_char(arg.expr, formatter.expr))


def date_format(arg: Expr, formatter: Expr) -> Expr:
    """Returns a string representation of a date, time, timestamp or duration.

    See Also:
        This is an alias for :py:func:`to_char`.
    """
    return to_char(arg, formatter)


def _unwrap_exprs(args: tuple[Expr, ...]) -> list:
    return [arg.expr for arg in args]


def to_date(arg: Expr, *formatters: Expr) -> Expr:
    """Converts a value to a date (YYYY-MM-DD).

    Supports strings, numeric and timestamp types as input.
    Integers and doubles are interpreted as days since the unix epoch.
    Strings are parsed as YYYY-MM-DD (e.g. '2023-07-20')
    if ``formatters`` are not provided.

    For usage of ``formatters`` see the rust chrono package ``strftime`` package.

    [Documentation here.](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["2021-07-20"]})
        >>> result = df.select(
        ...     dfn.functions.to_date(dfn.col("a")).alias("dt"))
        >>> str(result.collect_column("dt")[0].as_py())
        '2021-07-20'
    """
    return Expr(f.to_date(arg.expr, *_unwrap_exprs(formatters)))


def to_local_time(*args: Expr) -> Expr:
    """Converts a timestamp with a timezone to a timestamp without a timezone.

    This function handles daylight saving time changes.
    """
    return Expr(f.to_local_time(*_unwrap_exprs(args)))


def to_time(arg: Expr, *formatters: Expr) -> Expr:
    """Converts a value to a time. Supports strings and timestamps as input.

    If ``formatters`` is not provided strings are parsed as HH:MM:SS, HH:MM or
    HH:MM:SS.nnnnnnnnn;

    For usage of ``formatters`` see the rust chrono package ``strftime`` package.

    [Documentation here.](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["14:30:00"]})
        >>> result = df.select(
        ...     dfn.functions.to_time(dfn.col("a")).alias("t"))
        >>> str(result.collect_column("t")[0].as_py())
        '14:30:00'
    """
    return Expr(f.to_time(arg.expr, *_unwrap_exprs(formatters)))


def to_timestamp(arg: Expr, *formatters: Expr) -> Expr:
    """Converts a string and optional formats to a ``Timestamp`` in nanoseconds.

    For usage of ``formatters`` see the rust chrono package ``strftime`` package.

    [Documentation here.](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["2021-01-01T00:00:00"]})
        >>> result = df.select(
        ...     dfn.functions.to_timestamp(
        ...         dfn.col("a")
        ...     ).alias("ts")
        ... )
        >>> str(result.collect_column("ts")[0].as_py())
        '2021-01-01 00:00:00'
    """
    return Expr(f.to_timestamp(arg.expr, *_unwrap_exprs(formatters)))


def to_timestamp_millis(arg: Expr, *formatters: Expr) -> Expr:
    """Converts a string and optional formats to a ``Timestamp`` in milliseconds.

    See :py:func:`to_timestamp` for a description on how to use formatters.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["2021-01-01T00:00:00"]})
        >>> result = df.select(
        ...     dfn.functions.to_timestamp_millis(
        ...         dfn.col("a")
        ...     ).alias("ts")
        ... )
        >>> str(result.collect_column("ts")[0].as_py())
        '2021-01-01 00:00:00'
    """
    return Expr(f.to_timestamp_millis(arg.expr, *_unwrap_exprs(formatters)))


def to_timestamp_micros(arg: Expr, *formatters: Expr) -> Expr:
    """Converts a string and optional formats to a ``Timestamp`` in microseconds.

    See :py:func:`to_timestamp` for a description on how to use formatters.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["2021-01-01T00:00:00"]})
        >>> result = df.select(
        ...     dfn.functions.to_timestamp_micros(
        ...         dfn.col("a")
        ...     ).alias("ts")
        ... )
        >>> str(result.collect_column("ts")[0].as_py())
        '2021-01-01 00:00:00'
    """
    return Expr(f.to_timestamp_micros(arg.expr, *_unwrap_exprs(formatters)))


def to_timestamp_nanos(arg: Expr, *formatters: Expr) -> Expr:
    """Converts a string and optional formats to a ``Timestamp`` in nanoseconds.

    See :py:func:`to_timestamp` for a description on how to use formatters.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["2021-01-01T00:00:00"]})
        >>> result = df.select(
        ...     dfn.functions.to_timestamp_nanos(
        ...         dfn.col("a")
        ...     ).alias("ts")
        ... )
        >>> str(result.collect_column("ts")[0].as_py())
        '2021-01-01 00:00:00'
    """
    return Expr(f.to_timestamp_nanos(arg.expr, *_unwrap_exprs(formatters)))


def to_timestamp_seconds(arg: Expr, *formatters: Expr) -> Expr:
    """Converts a string and optional formats to a ``Timestamp`` in seconds.

    See :py:func:`to_timestamp` for a description on how to use formatters.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["2021-01-01T00:00:00"]})
        >>> result = df.select(
        ...     dfn.functions.to_timestamp_seconds(
        ...         dfn.col("a")
        ...     ).alias("ts")
        ... )
        >>> str(result.collect_column("ts")[0].as_py())
        '2021-01-01 00:00:00'
    """
    return Expr(f.to_timestamp_seconds(arg.expr, *_unwrap_exprs(formatters)))


def to_unixtime(string: Expr, *format_arguments: Expr) -> Expr:
    """Converts a string and optional formats to a Unixtime.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["1970-01-01T00:00:00"]})
        >>> result = df.select(dfn.functions.to_unixtime(dfn.col("a")).alias("u"))
        >>> result.collect_column("u")[0].as_py()
        0
    """
    return Expr(f.to_unixtime(string.expr, *_unwrap_exprs(format_arguments)))


def current_date() -> Expr:
    """Returns current UTC date as a Date32 value.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> result = df.select(
        ...     dfn.functions.current_date().alias("d")
        ... )
        >>> result.collect_column("d")[0].as_py() is not None
        True
    """
    return Expr(f.current_date())


today = current_date


def current_time() -> Expr:
    """Returns current UTC time as a Time64 value.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> result = df.select(
        ...     dfn.functions.current_time().alias("t")
        ... )

        Use .value instead of .as_py() because nanosecond timestamps
        require pandas to convert to Python datetime objects.

        >>> result.collect_column("t")[0].value > 0
        True
    """
    return Expr(f.current_time())


def datepart(part: Expr, date: Expr) -> Expr:
    """Return a specified part of a date.

    See Also:
        This is an alias for :py:func:`date_part`.
    """
    return date_part(part, date)


def date_part(part: Expr, date: Expr) -> Expr:
    """Extracts a subfield from the date.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["2021-07-15T00:00:00"]})
        >>> df = df.select(dfn.functions.to_timestamp(dfn.col("a")).alias("a"))
        >>> result = df.select(
        ...     dfn.functions.date_part(dfn.lit("year"), dfn.col("a")).alias("y"))
        >>> result.collect_column("y")[0].as_py()
        2021
    """
    return Expr(f.date_part(part.expr, date.expr))


def extract(part: Expr, date: Expr) -> Expr:
    """Extracts a subfield from the date.

    See Also:
        This is an alias for :py:func:`date_part`.
    """
    return date_part(part, date)


def date_trunc(part: Expr, date: Expr) -> Expr:
    """Truncates the date to a specified level of precision.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["2021-07-15T12:34:56"]})
        >>> df = df.select(dfn.functions.to_timestamp(dfn.col("a")).alias("a"))
        >>> result = df.select(
        ...     dfn.functions.date_trunc(
        ...         dfn.lit("month"), dfn.col("a")
        ...     ).alias("t")
        ... )
        >>> str(result.collect_column("t")[0].as_py())
        '2021-07-01 00:00:00'
    """
    return Expr(f.date_trunc(part.expr, date.expr))


def datetrunc(part: Expr, date: Expr) -> Expr:
    """Truncates the date to a specified level of precision.

    See Also:
        This is an alias for :py:func:`date_trunc`.
    """
    return date_trunc(part, date)


def date_bin(stride: Expr, source: Expr, origin: Expr) -> Expr:
    """Coerces an arbitrary timestamp to the start of the nearest specified interval.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"timestamp": ['2021-07-15 12:34:56', '2021-01-01']})
        >>> result = df.select(
        ...     dfn.functions.date_bin(
        ...         dfn.string_literal("15 minutes"),
        ...         dfn.col("timestamp"),
        ...         dfn.string_literal("2001-01-01 00:00:00")
        ...     ).alias("b")
        ... )
        >>> str(result.collect_column("b")[0].as_py())
        '2021-07-15 12:30:00'
        >>> str(result.collect_column("b")[1].as_py())
        '2021-01-01 00:00:00'
    """
    return Expr(f.date_bin(stride.expr, source.expr, origin.expr))


def make_date(year: Expr, month: Expr, day: Expr) -> Expr:
    """Make a date from year, month and day component parts.

    Examples:
        >>> from datetime import date
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"y": [2024], "m": [1], "d": [15]})
        >>> result = df.select(
        ...     dfn.functions.make_date(dfn.col("y"), dfn.col("m"),
        ...     dfn.col("d")).alias("dt"))
        >>> result.collect_column("dt")[0].as_py()
        datetime.date(2024, 1, 15)
    """
    return Expr(f.make_date(year.expr, month.expr, day.expr))


def make_time(hour: Expr, minute: Expr, second: Expr) -> Expr:
    """Make a time from hour, minute and second component parts.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"h": [12], "m": [30], "s": [0]})
        >>> result = df.select(
        ...     dfn.functions.make_time(dfn.col("h"), dfn.col("m"),
        ...     dfn.col("s")).alias("t"))
        >>> result.collect_column("t")[0].as_py()
        datetime.time(12, 30)
    """
    return Expr(f.make_time(hour.expr, minute.expr, second.expr))


def translate(string: Expr, from_val: Expr, to_val: Expr) -> Expr:
    """Replaces the characters in ``from_val`` with the counterpart in ``to_val``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(
        ...     dfn.functions.translate(dfn.col("a"), dfn.lit("helo"),
        ...     dfn.lit("HELO")).alias("t"))
        >>> result.collect_column("t")[0].as_py()
        'HELLO'
    """
    return Expr(f.translate(string.expr, from_val.expr, to_val.expr))


def trim(arg: Expr) -> Expr:
    """Removes all characters, spaces by default, from both sides of a string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["  hello  "]})
        >>> result = df.select(dfn.functions.trim(dfn.col("a")).alias("t"))
        >>> result.collect_column("t")[0].as_py()
        'hello'
    """
    return Expr(f.trim(arg.expr))


def trunc(num: Expr, precision: Expr | None = None) -> Expr:
    """Truncate the number toward zero with optional precision.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.567]})
        >>> result = df.select(
        ...     dfn.functions.trunc(
        ...         dfn.col("a")
        ...     ).alias("t"))
        >>> result.collect_column("t")[0].as_py()
        1.0

        >>> result = df.select(
        ...     dfn.functions.trunc(
        ...         dfn.col("a"), precision=dfn.lit(2)
        ...     ).alias("t"))
        >>> result.collect_column("t")[0].as_py()
        1.56
    """
    if precision is not None:
        return Expr(f.trunc(num.expr, precision.expr))
    return Expr(f.trunc(num.expr))


def upper(arg: Expr) -> Expr:
    """Converts a string to uppercase.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello"]})
        >>> result = df.select(dfn.functions.upper(dfn.col("a")).alias("u"))
        >>> result.collect_column("u")[0].as_py()
        'HELLO'
    """
    return Expr(f.upper(arg.expr))


def make_array(*args: Expr) -> Expr:
    """Returns an array using the specified input expressions.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> result = df.select(
        ...     dfn.functions.make_array(
        ...         dfn.lit(1), dfn.lit(2), dfn.lit(3)
        ...     ).alias("arr"))
        >>> result.collect_column("arr")[0].as_py()
        [1, 2, 3]
    """
    args = [arg.expr for arg in args]
    return Expr(f.make_array(args))


def make_list(*args: Expr) -> Expr:
    """Returns an array using the specified input expressions.

    See Also:
        This is an alias for :py:func:`make_array`.
    """
    return make_array(*args)


def array(*args: Expr) -> Expr:
    """Returns an array using the specified input expressions.

    See Also:
        This is an alias for :py:func:`make_array`.
    """
    return make_array(*args)


def range(start: Expr, stop: Expr, step: Expr) -> Expr:
    """Create a list of values in the range between start and stop.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> result = df.select(
        ...     dfn.functions.range(dfn.lit(0), dfn.lit(5), dfn.lit(2)).alias("r"))
        >>> result.collect_column("r")[0].as_py()
        [0, 2, 4]
    """
    return Expr(f.range(start.expr, stop.expr, step.expr))


def uuid() -> Expr:
    """Returns uuid v4 as a string value.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> result = df.select(
        ...     dfn.functions.uuid().alias("u")
        ... )
        >>> len(result.collect_column("u")[0].as_py()) == 36
        True
    """
    return Expr(f.uuid())


def struct(*args: Expr) -> Expr:
    """Returns a struct with the given arguments.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1], "b": [2]})
        >>> result = df.select(
        ...     dfn.functions.struct(
        ...         dfn.col("a"), dfn.col("b")
        ...     ).alias("s")
        ... )

        Children in the new struct will always be `c0`, ..., `cN-1`
        for `N` children.

        >>> result.collect_column("s")[0].as_py() == {"c0": 1, "c1": 2}
        True
    """
    args = [arg.expr for arg in args]
    return Expr(f.struct(*args))


def named_struct(name_pairs: list[tuple[str, Expr]]) -> Expr:
    """Returns a struct with the given names and arguments pairs.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> result = df.select(
        ...     dfn.functions.named_struct(
        ...         [("x", dfn.lit(10)), ("y", dfn.lit(20))]
        ...     ).alias("s")
        ... )
        >>> result.collect_column("s")[0].as_py() == {"x": 10, "y": 20}
        True
    """
    name_pair_exprs = [
        [Expr.literal(pa.scalar(pair[0], type=pa.string())), pair[1]]
        for pair in name_pairs
    ]

    # flatten
    name_pairs = [x.expr for xs in name_pair_exprs for x in xs]
    return Expr(f.named_struct(*name_pairs))


def from_unixtime(arg: Expr) -> Expr:
    """Converts an integer to RFC3339 timestamp format string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0]})
        >>> result = df.select(
        ...     dfn.functions.from_unixtime(
        ...         dfn.col("a")
        ...     ).alias("ts")
        ... )
        >>> str(result.collect_column("ts")[0].as_py())
        '1970-01-01 00:00:00'
    """
    return Expr(f.from_unixtime(arg.expr))


def arrow_typeof(arg: Expr) -> Expr:
    """Returns the Arrow type of the expression.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> result = df.select(dfn.functions.arrow_typeof(dfn.col("a")).alias("t"))
        >>> result.collect_column("t")[0].as_py()
        'Int64'
    """
    return Expr(f.arrow_typeof(arg.expr))


def arrow_cast(expr: Expr, data_type: Expr | str | pa.DataType) -> Expr:
    """Casts an expression to a specified data type.

    The ``data_type`` can be a string, a ``pyarrow.DataType``, or an
    ``Expr``. For simple types, :py:meth:`Expr.cast()
    <datafusion.expr.Expr.cast>` is more concise
    (e.g., ``col("a").cast(pa.float64())``). Use ``arrow_cast`` when
    you want to specify the target type as a string using DataFusion's
    type syntax, which can be more readable for complex types like
    ``"Timestamp(Nanosecond, None)"``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> result = df.select(
        ...     dfn.functions.arrow_cast(dfn.col("a"), "Float64").alias("c")
        ... )
        >>> result.collect_column("c")[0].as_py()
        1.0

        >>> result = df.select(
        ...     dfn.functions.arrow_cast(
        ...         dfn.col("a"), data_type=pa.float64()
        ...     ).alias("c")
        ... )
        >>> result.collect_column("c")[0].as_py()
        1.0
    """
    if isinstance(data_type, pa.DataType):
        return expr.cast(data_type)
    if isinstance(data_type, str):
        data_type = Expr.string_literal(data_type)
    return Expr(f.arrow_cast(expr.expr, data_type.expr))


def arrow_metadata(expr: Expr, key: Expr | str | None = None) -> Expr:
    """Returns the metadata of the input expression.

    If called with one argument, returns a Map of all metadata key-value pairs.
    If called with two arguments, returns the value for the specified metadata key.

    Examples:
        >>> field = pa.field("val", pa.int64(), metadata={"k": "v"})
        >>> schema = pa.schema([field])
        >>> batch = pa.RecordBatch.from_arrays([pa.array([1])], schema=schema)
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.create_dataframe([[batch]])
        >>> result = df.select(
        ...     dfn.functions.arrow_metadata(dfn.col("val")).alias("meta")
        ... )
        >>> ("k", "v") in result.collect_column("meta")[0].as_py()
        True

        >>> result = df.select(
        ...     dfn.functions.arrow_metadata(
        ...         dfn.col("val"), key="k"
        ...     ).alias("meta_val")
        ... )
        >>> result.collect_column("meta_val")[0].as_py()
        'v'
    """
    if key is None:
        return Expr(f.arrow_metadata(expr.expr))
    if isinstance(key, str):
        key = Expr.string_literal(key)
    return Expr(f.arrow_metadata(expr.expr, key.expr))


def get_field(expr: Expr, name: Expr | str) -> Expr:
    """Extracts a field from a struct or map by name.

    When the field name is a static string, the bracket operator
    ``expr["field"]`` is a convenient shorthand. Use ``get_field``
    when the field name is a dynamic expression.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1], "b": [2]})
        >>> df = df.with_column(
        ...     "s",
        ...     dfn.functions.named_struct(
        ...         [("x", dfn.col("a")), ("y", dfn.col("b"))]
        ...     ),
        ... )
        >>> result = df.select(
        ...     dfn.functions.get_field(dfn.col("s"), "x").alias("x_val")
        ... )
        >>> result.collect_column("x_val")[0].as_py()
        1

        Equivalent using bracket syntax:

        >>> result = df.select(
        ...     dfn.col("s")["x"].alias("x_val")
        ... )
        >>> result.collect_column("x_val")[0].as_py()
        1
    """
    if isinstance(name, str):
        name = Expr.string_literal(name)
    return Expr(f.get_field(expr.expr, name.expr))


def union_extract(union_expr: Expr, field_name: Expr | str) -> Expr:
    """Extracts a value from a union type by field name.

    Returns the value of the named field if it is the currently selected
    variant, otherwise returns NULL.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> types = pa.array([0, 1, 0], type=pa.int8())
        >>> offsets = pa.array([0, 0, 1], type=pa.int32())
        >>> arr = pa.UnionArray.from_dense(
        ...     types, offsets, [pa.array([1, 2]), pa.array(["hi"])],
        ...     ["int", "str"], [0, 1],
        ... )
        >>> batch = pa.RecordBatch.from_arrays([arr], names=["u"])
        >>> df = ctx.create_dataframe([[batch]])
        >>> result = df.select(
        ...     dfn.functions.union_extract(dfn.col("u"), "int").alias("val")
        ... )
        >>> result.collect_column("val").to_pylist()
        [1, None, 2]
    """
    if isinstance(field_name, str):
        field_name = Expr.string_literal(field_name)
    return Expr(f.union_extract(union_expr.expr, field_name.expr))


def union_tag(union_expr: Expr) -> Expr:
    """Returns the tag (active field name) of a union type.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> types = pa.array([0, 1, 0], type=pa.int8())
        >>> offsets = pa.array([0, 0, 1], type=pa.int32())
        >>> arr = pa.UnionArray.from_dense(
        ...     types, offsets, [pa.array([1, 2]), pa.array(["hi"])],
        ...     ["int", "str"], [0, 1],
        ... )
        >>> batch = pa.RecordBatch.from_arrays([arr], names=["u"])
        >>> df = ctx.create_dataframe([[batch]])
        >>> result = df.select(
        ...     dfn.functions.union_tag(dfn.col("u")).alias("tag")
        ... )
        >>> result.collect_column("tag").to_pylist()
        ['int', 'str', 'int']
    """
    return Expr(f.union_tag(union_expr.expr))


def version() -> Expr:
    """Returns the DataFusion version string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.empty_table()
        >>> result = df.select(dfn.functions.version().alias("v"))
        >>> "Apache DataFusion" in result.collect_column("v")[0].as_py()
        True
    """
    return Expr(f.version())


def row(*args: Expr) -> Expr:
    """Returns a struct with the given arguments.

    See Also:
        This is an alias for :py:func:`struct`.
    """
    return struct(*args)


def random() -> Expr:
    """Returns a random value in the range ``0.0 <= x < 1.0``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> result = df.select(
        ...     dfn.functions.random().alias("r")
        ... )
        >>> val = result.collect_column("r")[0].as_py()
        >>> 0.0 <= val < 1.0
        True
    """
    return Expr(f.random())


def array_append(array: Expr, element: Expr) -> Expr:
    """Appends an element to the end of an array.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]]})
        >>> result = df.select(
        ...     dfn.functions.array_append(dfn.col("a"), dfn.lit(4)).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [1, 2, 3, 4]
    """
    return Expr(f.array_append(array.expr, element.expr))


def array_push_back(array: Expr, element: Expr) -> Expr:
    """Appends an element to the end of an array.

    See Also:
        This is an alias for :py:func:`array_append`.
    """
    return array_append(array, element)


def list_append(array: Expr, element: Expr) -> Expr:
    """Appends an element to the end of an array.

    See Also:
        This is an alias for :py:func:`array_append`.
    """
    return array_append(array, element)


def list_push_back(array: Expr, element: Expr) -> Expr:
    """Appends an element to the end of an array.

    See Also:
        This is an alias for :py:func:`array_append`.
    """
    return array_append(array, element)


def array_concat(*args: Expr) -> Expr:
    """Concatenates the input arrays.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2]], "b": [[3, 4]]})
        >>> result = df.select(
        ...     dfn.functions.array_concat(dfn.col("a"), dfn.col("b")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [1, 2, 3, 4]
    """
    args = [arg.expr for arg in args]
    return Expr(f.array_concat(args))


def array_cat(*args: Expr) -> Expr:
    """Concatenates the input arrays.

    See Also:
        This is an alias for :py:func:`array_concat`.
    """
    return array_concat(*args)


def array_dims(array: Expr) -> Expr:
    """Returns an array of the array's dimensions.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]]})
        >>> result = df.select(dfn.functions.array_dims(dfn.col("a")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [3]
    """
    return Expr(f.array_dims(array.expr))


def array_distinct(array: Expr) -> Expr:
    """Returns distinct values from the array after removing duplicates.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 1, 2, 3]]})
        >>> result = df.select(
        ...     dfn.functions.array_distinct(
        ...         dfn.col("a")
        ...     ).alias("result")
        ... )
        >>> sorted(
        ...     result.collect_column("result")[0].as_py()
        ... )
        [1, 2, 3]
    """
    return Expr(f.array_distinct(array.expr))


def list_cat(*args: Expr) -> Expr:
    """Concatenates the input arrays.

    See Also:
        This is an alias for :py:func:`array_concat`, :py:func:`array_cat`.
    """
    return array_concat(*args)


def list_concat(*args: Expr) -> Expr:
    """Concatenates the input arrays.

    See Also:
        This is an alias for :py:func:`array_concat`, :py:func:`array_cat`.
    """
    return array_concat(*args)


def list_distinct(array: Expr) -> Expr:
    """Returns distinct values from the array after removing duplicates.

    See Also:
        This is an alias for :py:func:`array_distinct`.
    """
    return array_distinct(array)


def list_dims(array: Expr) -> Expr:
    """Returns an array of the array's dimensions.

    See Also:
        This is an alias for :py:func:`array_dims`.
    """
    return array_dims(array)


def array_element(array: Expr, n: Expr) -> Expr:
    """Extracts the element with the index n from the array.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[10, 20, 30]]})
        >>> result = df.select(
        ...     dfn.functions.array_element(dfn.col("a"), dfn.lit(2)).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        20
    """
    return Expr(f.array_element(array.expr, n.expr))


def array_empty(array: Expr) -> Expr:
    """Returns a boolean indicating whether the array is empty.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2]]})
        >>> result = df.select(dfn.functions.array_empty(dfn.col("a")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        False
    """
    return Expr(f.array_empty(array.expr))


def list_empty(array: Expr) -> Expr:
    """Returns a boolean indicating whether the array is empty.

    See Also:
        This is an alias for :py:func:`array_empty`.
    """
    return array_empty(array)


def array_extract(array: Expr, n: Expr) -> Expr:
    """Extracts the element with the index n from the array.

    See Also:
        This is an alias for :py:func:`array_element`.
    """
    return array_element(array, n)


def list_element(array: Expr, n: Expr) -> Expr:
    """Extracts the element with the index n from the array.

    See Also:
        This is an alias for :py:func:`array_element`.
    """
    return array_element(array, n)


def list_extract(array: Expr, n: Expr) -> Expr:
    """Extracts the element with the index n from the array.

    See Also:
        This is an alias for :py:func:`array_element`.
    """
    return array_element(array, n)


def array_length(array: Expr) -> Expr:
    """Returns the length of the array.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]]})
        >>> result = df.select(dfn.functions.array_length(dfn.col("a")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        3
    """
    return Expr(f.array_length(array.expr))


def list_length(array: Expr) -> Expr:
    """Returns the length of the array.

    See Also:
        This is an alias for :py:func:`array_length`.
    """
    return array_length(array)


def array_has(first_array: Expr, second_array: Expr) -> Expr:
    """Returns true if the element appears in the first array, otherwise false.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]]})
        >>> result = df.select(
        ...     dfn.functions.array_has(dfn.col("a"), dfn.lit(2)).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        True
    """
    return Expr(f.array_has(first_array.expr, second_array.expr))


def array_has_all(first_array: Expr, second_array: Expr) -> Expr:
    """Determines if there is complete overlap ``second_array`` in ``first_array``.

    Returns true if each element of the second array appears in the first array.
    Otherwise, it returns false.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]], "b": [[1, 2]]})
        >>> result = df.select(
        ...     dfn.functions.array_has_all(dfn.col("a"), dfn.col("b")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        True
    """
    return Expr(f.array_has_all(first_array.expr, second_array.expr))


def array_has_any(first_array: Expr, second_array: Expr) -> Expr:
    """Determine if there is an overlap between ``first_array`` and ``second_array``.

    Returns true if at least one element of the second array appears in the first
    array. Otherwise, it returns false.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]], "b": [[2, 5]]})
        >>> result = df.select(
        ...     dfn.functions.array_has_any(dfn.col("a"), dfn.col("b")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        True
    """
    return Expr(f.array_has_any(first_array.expr, second_array.expr))


def array_contains(array: Expr, element: Expr) -> Expr:
    """Returns true if the element appears in the array, otherwise false.

    See Also:
        This is an alias for :py:func:`array_has`.
    """
    return array_has(array, element)


def list_has(array: Expr, element: Expr) -> Expr:
    """Returns true if the element appears in the array, otherwise false.

    See Also:
        This is an alias for :py:func:`array_has`.
    """
    return array_has(array, element)


def list_has_all(first_array: Expr, second_array: Expr) -> Expr:
    """Determines if there is complete overlap ``second_array`` in ``first_array``.

    See Also:
        This is an alias for :py:func:`array_has_all`.
    """
    return array_has_all(first_array, second_array)


def list_has_any(first_array: Expr, second_array: Expr) -> Expr:
    """Determine if there is an overlap between ``first_array`` and ``second_array``.

    See Also:
        This is an alias for :py:func:`array_has_any`.
    """
    return array_has_any(first_array, second_array)


def arrays_overlap(first_array: Expr, second_array: Expr) -> Expr:
    """Returns true if any element appears in both arrays.

    See Also:
        This is an alias for :py:func:`array_has_any`.
    """
    return array_has_any(first_array, second_array)


def list_overlap(first_array: Expr, second_array: Expr) -> Expr:
    """Returns true if any element appears in both arrays.

    See Also:
        This is an alias for :py:func:`array_has_any`.
    """
    return array_has_any(first_array, second_array)


def list_contains(array: Expr, element: Expr) -> Expr:
    """Returns true if the element appears in the array, otherwise false.

    See Also:
        This is an alias for :py:func:`array_has`.
    """
    return array_has(array, element)


def array_position(array: Expr, element: Expr, index: int | None = 1) -> Expr:
    """Return the position of the first occurrence of ``element`` in ``array``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[10, 20, 30]]})
        >>> result = df.select(
        ...     dfn.functions.array_position(
        ...         dfn.col("a"), dfn.lit(20)
        ...     ).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        2

        Use ``index`` to start searching from a given position:

        >>> df = ctx.from_pydict({"a": [[10, 20, 10, 20]]})
        >>> result = df.select(
        ...     dfn.functions.array_position(
        ...         dfn.col("a"), dfn.lit(20), index=3,
        ...     ).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        4
    """
    return Expr(f.array_position(array.expr, element.expr, index))


def array_indexof(array: Expr, element: Expr, index: int | None = 1) -> Expr:
    """Return the position of the first occurrence of ``element`` in ``array``.

    See Also:
        This is an alias for :py:func:`array_position`.
    """
    return array_position(array, element, index)


def list_position(array: Expr, element: Expr, index: int | None = 1) -> Expr:
    """Return the position of the first occurrence of ``element`` in ``array``.

    See Also:
        This is an alias for :py:func:`array_position`.
    """
    return array_position(array, element, index)


def list_indexof(array: Expr, element: Expr, index: int | None = 1) -> Expr:
    """Return the position of the first occurrence of ``element`` in ``array``.

    See Also:
        This is an alias for :py:func:`array_position`.
    """
    return array_position(array, element, index)


def array_positions(array: Expr, element: Expr) -> Expr:
    """Searches for an element in the array and returns all occurrences.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 1]]})
        >>> result = df.select(
        ...     dfn.functions.array_positions(dfn.col("a"), dfn.lit(1)).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [1, 3]
    """
    return Expr(f.array_positions(array.expr, element.expr))


def list_positions(array: Expr, element: Expr) -> Expr:
    """Searches for an element in the array and returns all occurrences.

    See Also:
        This is an alias for :py:func:`array_positions`.
    """
    return array_positions(array, element)


def array_ndims(array: Expr) -> Expr:
    """Returns the number of dimensions of the array.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]]})
        >>> result = df.select(dfn.functions.array_ndims(dfn.col("a")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        1
    """
    return Expr(f.array_ndims(array.expr))


def list_ndims(array: Expr) -> Expr:
    """Returns the number of dimensions of the array.

    See Also:
        This is an alias for :py:func:`array_ndims`.
    """
    return array_ndims(array)


def array_prepend(element: Expr, array: Expr) -> Expr:
    """Prepends an element to the beginning of an array.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2]]})
        >>> result = df.select(
        ...     dfn.functions.array_prepend(dfn.lit(0), dfn.col("a")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [0, 1, 2]
    """
    return Expr(f.array_prepend(element.expr, array.expr))


def array_push_front(element: Expr, array: Expr) -> Expr:
    """Prepends an element to the beginning of an array.

    See Also:
        This is an alias for :py:func:`array_prepend`.
    """
    return array_prepend(element, array)


def list_prepend(element: Expr, array: Expr) -> Expr:
    """Prepends an element to the beginning of an array.

    See Also:
        This is an alias for :py:func:`array_prepend`.
    """
    return array_prepend(element, array)


def list_push_front(element: Expr, array: Expr) -> Expr:
    """Prepends an element to the beginning of an array.

    See Also:
        This is an alias for :py:func:`array_prepend`.
    """
    return array_prepend(element, array)


def array_pop_back(array: Expr) -> Expr:
    """Returns the array without the last element.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]]})
        >>> result = df.select(
        ...     dfn.functions.array_pop_back(dfn.col("a")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [1, 2]
    """
    return Expr(f.array_pop_back(array.expr))


def array_pop_front(array: Expr) -> Expr:
    """Returns the array without the first element.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]]})
        >>> result = df.select(
        ...     dfn.functions.array_pop_front(dfn.col("a")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [2, 3]
    """
    return Expr(f.array_pop_front(array.expr))


def list_pop_back(array: Expr) -> Expr:
    """Returns the array without the last element.

    See Also:
        This is an alias for :py:func:`array_pop_back`.
    """
    return array_pop_back(array)


def list_pop_front(array: Expr) -> Expr:
    """Returns the array without the first element.

    See Also:
        This is an alias for :py:func:`array_pop_front`.
    """
    return array_pop_front(array)


def array_remove(array: Expr, element: Expr) -> Expr:
    """Removes the first element from the array equal to the given value.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 1]]})
        >>> result = df.select(
        ...     dfn.functions.array_remove(dfn.col("a"), dfn.lit(1)).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [2, 1]
    """
    return Expr(f.array_remove(array.expr, element.expr))


def list_remove(array: Expr, element: Expr) -> Expr:
    """Removes the first element from the array equal to the given value.

    See Also:
        This is an alias for :py:func:`array_remove`.
    """
    return array_remove(array, element)


def array_remove_n(array: Expr, element: Expr, max: Expr) -> Expr:
    """Removes the first ``max`` elements from the array equal to the given value.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 1, 1]]})
        >>> result = df.select(
        ...     dfn.functions.array_remove_n(dfn.col("a"), dfn.lit(1),
        ...     dfn.lit(2)).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [2, 1]
    """
    return Expr(f.array_remove_n(array.expr, element.expr, max.expr))


def list_remove_n(array: Expr, element: Expr, max: Expr) -> Expr:
    """Removes the first ``max`` elements from the array equal to the given value.

    See Also:
        This is an alias for :py:func:`array_remove_n`.
    """
    return array_remove_n(array, element, max)


def array_remove_all(array: Expr, element: Expr) -> Expr:
    """Removes all elements from the array equal to the given value.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 1]]})
        >>> result = df.select(
        ...     dfn.functions.array_remove_all(
        ...         dfn.col("a"), dfn.lit(1)
        ...     ).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [2]
    """
    return Expr(f.array_remove_all(array.expr, element.expr))


def list_remove_all(array: Expr, element: Expr) -> Expr:
    """Removes all elements from the array equal to the given value.

    See Also:
        This is an alias for :py:func:`array_remove_all`.
    """
    return array_remove_all(array, element)


def array_repeat(element: Expr, count: Expr) -> Expr:
    """Returns an array containing ``element`` ``count`` times.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> result = df.select(
        ...     dfn.functions.array_repeat(dfn.lit(3), dfn.lit(3)).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [3, 3, 3]
    """
    return Expr(f.array_repeat(element.expr, count.expr))


def list_repeat(element: Expr, count: Expr) -> Expr:
    """Returns an array containing ``element`` ``count`` times.

    See Also:
        This is an alias for :py:func:`array_repeat`.
    """
    return array_repeat(element, count)


def array_replace(array: Expr, from_val: Expr, to_val: Expr) -> Expr:
    """Replaces the first occurrence of ``from_val`` with ``to_val``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 1]]})
        >>> result = df.select(
        ...     dfn.functions.array_replace(dfn.col("a"), dfn.lit(1),
        ...     dfn.lit(9)).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [9, 2, 1]
    """
    return Expr(f.array_replace(array.expr, from_val.expr, to_val.expr))


def list_replace(array: Expr, from_val: Expr, to_val: Expr) -> Expr:
    """Replaces the first occurrence of ``from_val`` with ``to_val``.

    See Also:
        This is an alias for :py:func:`array_replace`.
    """
    return array_replace(array, from_val, to_val)


def array_replace_n(array: Expr, from_val: Expr, to_val: Expr, max: Expr) -> Expr:
    """Replace ``n`` occurrences of ``from_val`` with ``to_val``.

    Replaces the first ``max`` occurrences of the specified element with another
    specified element.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 1, 1]]})
        >>> result = df.select(
        ...     dfn.functions.array_replace_n(dfn.col("a"), dfn.lit(1), dfn.lit(9),
        ...     dfn.lit(2)).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [9, 2, 9, 1]
    """
    return Expr(f.array_replace_n(array.expr, from_val.expr, to_val.expr, max.expr))


def list_replace_n(array: Expr, from_val: Expr, to_val: Expr, max: Expr) -> Expr:
    """Replace ``n`` occurrences of ``from_val`` with ``to_val``.

    Replaces the first ``max`` occurrences of the specified element with another
    specified element.

    See Also:
        This is an alias for :py:func:`array_replace_n`.
    """
    return array_replace_n(array, from_val, to_val, max)


def array_replace_all(array: Expr, from_val: Expr, to_val: Expr) -> Expr:
    """Replaces all occurrences of ``from_val`` with ``to_val``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 1]]})
        >>> result = df.select(
        ...     dfn.functions.array_replace_all(dfn.col("a"), dfn.lit(1),
        ...     dfn.lit(9)).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [9, 2, 9]
    """
    return Expr(f.array_replace_all(array.expr, from_val.expr, to_val.expr))


def list_replace_all(array: Expr, from_val: Expr, to_val: Expr) -> Expr:
    """Replaces all occurrences of ``from_val`` with ``to_val``.

    See Also:
        This is an alias for :py:func:`array_replace_all`.
    """
    return array_replace_all(array, from_val, to_val)


def array_sort(array: Expr, descending: bool = False, null_first: bool = False) -> Expr:
    """Sort an array.

    Args:
        array: The input array to sort.
        descending: If True, sorts in descending order.
        null_first: If True, nulls will be returned at the beginning of the array.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[3, 1, 2]]})
        >>> result = df.select(
        ...     dfn.functions.array_sort(
        ...         dfn.col("a")
        ...     ).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [1, 2, 3]

        >>> df = ctx.from_pydict({"a": [[3, None, 1]]})
        >>> result = df.select(
        ...     dfn.functions.array_sort(
        ...         dfn.col("a"), descending=True, null_first=True,
        ...     ).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [None, 3, 1]
    """
    desc = "DESC" if descending else "ASC"
    nulls_first = "NULLS FIRST" if null_first else "NULLS LAST"
    return Expr(
        f.array_sort(
            array.expr,
            Expr.literal(pa.scalar(desc, type=pa.string())).expr,
            Expr.literal(pa.scalar(nulls_first, type=pa.string())).expr,
        )
    )


def list_sort(array: Expr, descending: bool = False, null_first: bool = False) -> Expr:
    """Sorts the array.

    See Also:
        This is an alias for :py:func:`array_sort`.
    """
    return array_sort(array, descending=descending, null_first=null_first)


def array_slice(
    array: Expr, begin: Expr, end: Expr, stride: Expr | None = None
) -> Expr:
    """Returns a slice of the array.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3, 4]]})
        >>> result = df.select(
        ...     dfn.functions.array_slice(
        ...         dfn.col("a"), dfn.lit(2), dfn.lit(3)
        ...     ).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [2, 3]

        Use ``stride`` to skip elements:

        >>> result = df.select(
        ...     dfn.functions.array_slice(
        ...         dfn.col("a"), dfn.lit(1), dfn.lit(4),
        ...         stride=dfn.lit(2),
        ...     ).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [1, 3]
    """
    if stride is not None:
        stride = stride.expr
    return Expr(f.array_slice(array.expr, begin.expr, end.expr, stride))


def list_slice(array: Expr, begin: Expr, end: Expr, stride: Expr | None = None) -> Expr:
    """Returns a slice of the array.

    See Also:
        This is an alias for :py:func:`array_slice`.
    """
    return array_slice(array, begin, end, stride)


def array_intersect(array1: Expr, array2: Expr) -> Expr:
    """Returns the intersection of ``array1`` and ``array2``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]], "b": [[2, 3, 4]]})
        >>> result = df.select(
        ...     dfn.functions.array_intersect(
        ...         dfn.col("a"), dfn.col("b")
        ...     ).alias("result")
        ... )
        >>> sorted(
        ...     result.collect_column("result")[0].as_py()
        ... )
        [2, 3]
    """
    return Expr(f.array_intersect(array1.expr, array2.expr))


def list_intersect(array1: Expr, array2: Expr) -> Expr:
    """Returns an the intersection of ``array1`` and ``array2``.

    See Also:
        This is an alias for :py:func:`array_intersect`.
    """
    return array_intersect(array1, array2)


def array_union(array1: Expr, array2: Expr) -> Expr:
    """Returns an array of the elements in the union of array1 and array2.

    Duplicate rows will not be returned.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]], "b": [[2, 3, 4]]})
        >>> result = df.select(
        ...     dfn.functions.array_union(
        ...         dfn.col("a"), dfn.col("b")
        ...     ).alias("result")
        ... )
        >>> sorted(
        ...     result.collect_column("result")[0].as_py()
        ... )
        [1, 2, 3, 4]
    """
    return Expr(f.array_union(array1.expr, array2.expr))


def list_union(array1: Expr, array2: Expr) -> Expr:
    """Returns an array of the elements in the union of array1 and array2.

    Duplicate rows will not be returned.

    See Also:
        This is an alias for :py:func:`array_union`.
    """
    return array_union(array1, array2)


def array_except(array1: Expr, array2: Expr) -> Expr:
    """Returns the elements that appear in ``array1`` but not in ``array2``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]], "b": [[2, 3, 4]]})
        >>> result = df.select(
        ...     dfn.functions.array_except(dfn.col("a"), dfn.col("b")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [1]
    """
    return Expr(f.array_except(array1.expr, array2.expr))


def list_except(array1: Expr, array2: Expr) -> Expr:
    """Returns the elements that appear in ``array1`` but not in the ``array2``.

    See Also:
        This is an alias for :py:func:`array_except`.
    """
    return array_except(array1, array2)


def array_resize(array: Expr, size: Expr, value: Expr) -> Expr:
    """Returns an array with the specified size filled.

    If ``size`` is greater than the ``array`` length, the additional entries will
    be filled with the given ``value``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2]]})
        >>> result = df.select(
        ...     dfn.functions.array_resize(dfn.col("a"), dfn.lit(4),
        ...     dfn.lit(0)).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [1, 2, 0, 0]
    """
    return Expr(f.array_resize(array.expr, size.expr, value.expr))


def list_resize(array: Expr, size: Expr, value: Expr) -> Expr:
    """Returns an array with the specified size filled.

    If ``size`` is greater than the ``array`` length, the additional entries will be
    filled with the given ``value``.

    See Also:
        This is an alias for :py:func:`array_resize`.
    """
    return array_resize(array, size, value)


def array_any_value(array: Expr) -> Expr:
    """Returns the first non-null element in the array.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[None, 2, 3]]})
        >>> result = df.select(
        ...     dfn.functions.array_any_value(dfn.col("a")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        2
    """
    return Expr(f.array_any_value(array.expr))


def list_any_value(array: Expr) -> Expr:
    """Returns the first non-null element in the array.

    See Also:
        This is an alias for :py:func:`array_any_value`.
    """
    return array_any_value(array)


def array_distance(array1: Expr, array2: Expr) -> Expr:
    """Returns the Euclidean distance between two numeric arrays.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1.0, 2.0]], "b": [[1.0, 4.0]]})
        >>> result = df.select(
        ...     dfn.functions.array_distance(
        ...         dfn.col("a"), dfn.col("b"),
        ...     ).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        2.0
    """
    return Expr(f.array_distance(array1.expr, array2.expr))


def list_distance(array1: Expr, array2: Expr) -> Expr:
    """Returns the Euclidean distance between two numeric arrays.

    See Also:
        This is an alias for :py:func:`array_distance`.
    """
    return array_distance(array1, array2)


def array_max(array: Expr) -> Expr:
    """Returns the maximum value in the array.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]]})
        >>> result = df.select(
        ...     dfn.functions.array_max(dfn.col("a")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        3
    """
    return Expr(f.array_max(array.expr))


def list_max(array: Expr) -> Expr:
    """Returns the maximum value in the array.

    See Also:
        This is an alias for :py:func:`array_max`.
    """
    return array_max(array)


def array_min(array: Expr) -> Expr:
    """Returns the minimum value in the array.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]]})
        >>> result = df.select(
        ...     dfn.functions.array_min(dfn.col("a")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        1
    """
    return Expr(f.array_min(array.expr))


def list_min(array: Expr) -> Expr:
    """Returns the minimum value in the array.

    See Also:
        This is an alias for :py:func:`array_min`.
    """
    return array_min(array)


def array_reverse(array: Expr) -> Expr:
    """Reverses the order of elements in the array.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]]})
        >>> result = df.select(
        ...     dfn.functions.array_reverse(dfn.col("a")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [3, 2, 1]
    """
    return Expr(f.array_reverse(array.expr))


def list_reverse(array: Expr) -> Expr:
    """Reverses the order of elements in the array.

    See Also:
        This is an alias for :py:func:`array_reverse`.
    """
    return array_reverse(array)


def arrays_zip(*arrays: Expr) -> Expr:
    """Combines multiple arrays into a single array of structs.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2]], "b": [[3, 4]]})
        >>> result = df.select(
        ...     dfn.functions.arrays_zip(dfn.col("a"), dfn.col("b")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [{'c0': 1, 'c1': 3}, {'c0': 2, 'c1': 4}]
    """
    args = [a.expr for a in arrays]
    return Expr(f.arrays_zip(args))


def list_zip(*arrays: Expr) -> Expr:
    """Combines multiple arrays into a single array of structs.

    See Also:
        This is an alias for :py:func:`arrays_zip`.
    """
    return arrays_zip(*arrays)


def string_to_array(
    string: Expr, delimiter: Expr, null_string: Expr | None = None
) -> Expr:
    """Splits a string based on a delimiter and returns an array of parts.

    Any parts matching the optional ``null_string`` will be replaced with ``NULL``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["hello,world"]})
        >>> result = df.select(
        ...     dfn.functions.string_to_array(
        ...         dfn.col("a"), dfn.lit(","),
        ...     ).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        ['hello', 'world']

        Replace parts matching a ``null_string`` with ``NULL``:

        >>> result = df.select(
        ...     dfn.functions.string_to_array(
        ...         dfn.col("a"), dfn.lit(","), null_string=dfn.lit("world"),
        ...     ).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        ['hello', None]
    """
    null_expr = null_string.expr if null_string is not None else None
    return Expr(f.string_to_array(string.expr, delimiter.expr, null_expr))


def string_to_list(
    string: Expr, delimiter: Expr, null_string: Expr | None = None
) -> Expr:
    """Splits a string based on a delimiter and returns an array of parts.

    See Also:
        This is an alias for :py:func:`string_to_array`.
    """
    return string_to_array(string, delimiter, null_string)


def gen_series(start: Expr, stop: Expr, step: Expr | None = None) -> Expr:
    """Creates a list of values in the range between start and stop.

    Unlike :py:func:`range`, this includes the upper bound.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0]})
        >>> result = df.select(
        ...     dfn.functions.gen_series(
        ...         dfn.lit(1), dfn.lit(5),
        ...     ).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [1, 2, 3, 4, 5]

        Specify a custom ``step``:

        >>> result = df.select(
        ...     dfn.functions.gen_series(
        ...         dfn.lit(1), dfn.lit(10), step=dfn.lit(3),
        ...     ).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [1, 4, 7, 10]
    """
    step_expr = step.expr if step is not None else None
    return Expr(f.gen_series(start.expr, stop.expr, step_expr))


def generate_series(start: Expr, stop: Expr, step: Expr | None = None) -> Expr:
    """Creates a list of values in the range between start and stop.

    Unlike :py:func:`range`, this includes the upper bound.

    See Also:
        This is an alias for :py:func:`gen_series`.
    """
    return gen_series(start, stop, step)


def flatten(array: Expr) -> Expr:
    """Flattens an array of arrays into a single array.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[[1, 2], [3, 4]]]})
        >>> result = df.select(dfn.functions.flatten(dfn.col("a")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        [1, 2, 3, 4]
    """
    return Expr(f.flatten(array.expr))


def cardinality(array: Expr) -> Expr:
    """Returns the total number of elements in the array.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [[1, 2, 3]]})
        >>> result = df.select(dfn.functions.cardinality(dfn.col("a")).alias("result"))
        >>> result.collect_column("result")[0].as_py()
        3
    """
    return Expr(f.cardinality(array.expr))


def empty(array: Expr) -> Expr:
    """Returns true if the array is empty.

    See Also:
        This is an alias for :py:func:`array_empty`.
    """
    return array_empty(array)


# map functions


def make_map(*args: Any) -> Expr:
    """Returns a map expression.

    Supports three calling conventions:

    - ``make_map({"a": 1, "b": 2})`` — from a Python dictionary.
    - ``make_map([keys], [values])`` — from a list of keys and a list of
      their associated values.  Both lists must be the same length.
    - ``make_map(k1, v1, k2, v2, ...)`` — from alternating keys and their
      associated values.

    Keys and values that are not already :py:class:`~datafusion.expr.Expr`
    are automatically converted to literal expressions.

    Examples:
        From a dictionary:

        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> result = df.select(
        ...     dfn.functions.make_map({"a": 1, "b": 2}).alias("m"))
        >>> result.collect_column("m")[0].as_py()
        [('a', 1), ('b', 2)]

        From two lists:

        >>> df = ctx.from_pydict({"key": ["x", "y"], "val": [10, 20]})
        >>> df = df.select(
        ...     dfn.functions.make_map(
        ...         [dfn.col("key")], [dfn.col("val")]
        ...     ).alias("m"))
        >>> df.collect_column("m")[0].as_py()
        [('x', 10)]

        From alternating keys and values:

        >>> df = ctx.from_pydict({"a": [1]})
        >>> result = df.select(
        ...     dfn.functions.make_map("x", 1, "y", 2).alias("m"))
        >>> result.collect_column("m")[0].as_py()
        [('x', 1), ('y', 2)]
    """
    if len(args) == 1 and isinstance(args[0], dict):
        key_list = list(args[0].keys())
        value_list = list(args[0].values())
    elif (
        len(args) == 2  # noqa: PLR2004
        and isinstance(args[0], list)
        and isinstance(args[1], list)
    ):
        if len(args[0]) != len(args[1]):
            msg = "make_map requires key and value lists to be the same length"
            raise ValueError(msg)
        key_list = args[0]
        value_list = args[1]
    elif len(args) >= 2 and len(args) % 2 == 0:  # noqa: PLR2004
        key_list = list(args[0::2])
        value_list = list(args[1::2])
    else:
        msg = (
            "make_map expects a dict, two lists, or an even number of "
            "key-value arguments"
        )
        raise ValueError(msg)

    key_exprs = [k if isinstance(k, Expr) else Expr.literal(k) for k in key_list]
    val_exprs = [v if isinstance(v, Expr) else Expr.literal(v) for v in value_list]
    return Expr(f.make_map([k.expr for k in key_exprs], [v.expr for v in val_exprs]))


def map_keys(map: Expr) -> Expr:
    """Returns a list of all keys in the map.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> df = df.select(
        ...     dfn.functions.make_map({"x": 1, "y": 2}).alias("m"))
        >>> result = df.select(
        ...     dfn.functions.map_keys(dfn.col("m")).alias("keys"))
        >>> result.collect_column("keys")[0].as_py()
        ['x', 'y']
    """
    return Expr(f.map_keys(map.expr))


def map_values(map: Expr) -> Expr:
    """Returns a list of all values in the map.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> df = df.select(
        ...     dfn.functions.make_map({"x": 1, "y": 2}).alias("m"))
        >>> result = df.select(
        ...     dfn.functions.map_values(dfn.col("m")).alias("vals"))
        >>> result.collect_column("vals")[0].as_py()
        [1, 2]
    """
    return Expr(f.map_values(map.expr))


def map_extract(map: Expr, key: Expr) -> Expr:
    """Returns the value for a given key in the map.

    Returns ``[None]`` if the key is absent.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> df = df.select(
        ...     dfn.functions.make_map({"x": 1, "y": 2}).alias("m"))
        >>> result = df.select(
        ...     dfn.functions.map_extract(
        ...         dfn.col("m"), dfn.lit("x")
        ...     ).alias("val"))
        >>> result.collect_column("val")[0].as_py()
        [1]
    """
    return Expr(f.map_extract(map.expr, key.expr))


def map_entries(map: Expr) -> Expr:
    """Returns a list of all entries (key-value struct pairs) in the map.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1]})
        >>> df = df.select(
        ...     dfn.functions.make_map({"x": 1, "y": 2}).alias("m"))
        >>> result = df.select(
        ...     dfn.functions.map_entries(dfn.col("m")).alias("entries"))
        >>> result.collect_column("entries")[0].as_py()
        [{'key': 'x', 'value': 1}, {'key': 'y', 'value': 2}]
    """
    return Expr(f.map_entries(map.expr))


def element_at(map: Expr, key: Expr) -> Expr:
    """Returns the value for a given key in the map.

    Returns ``[None]`` if the key is absent.

    See Also:
        This is an alias for :py:func:`map_extract`.
    """
    return map_extract(map, key)


# aggregate functions
def approx_distinct(
    expression: Expr,
    filter: Expr | None = None,
) -> Expr:
    """Returns the approximate number of distinct values.

    This aggregate function is similar to :py:func:`count` with distinct set, but it
    will approximate the number of distinct entries. It may return significantly faster
    than :py:func:`count` for some DataFrames.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: Values to check for distinct entries
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 1, 2, 3]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.approx_distinct(
        ...         dfn.col("a")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py() == 3
        True

        >>> result = df.aggregate(
        ...     [], [dfn.functions.approx_distinct(
        ...         dfn.col("a"),
        ...         filter=dfn.col("a") > dfn.lit(1)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py() == 2
        True
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.approx_distinct(expression.expr, filter=filter_raw))


def approx_median(expression: Expr, filter: Expr | None = None) -> Expr:
    """Returns the approximate median value.

    This aggregate function is similar to :py:func:`median`, but it will only
    approximate the median. It may return significantly faster for some DataFrames.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by`` and ``null_treatment``, and ``distinct``.

    Args:
        expression: Values to find the median for
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.0, 2.0, 3.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.approx_median(
        ...         dfn.col("a")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2.0

        >>> result = df.aggregate(
        ...     [], [dfn.functions.approx_median(
        ...         dfn.col("a"),
        ...         filter=dfn.col("a") > dfn.lit(1.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2.5
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.approx_median(expression.expr, filter=filter_raw))


def approx_percentile_cont(
    sort_expression: Expr | SortExpr,
    percentile: float,
    num_centroids: int | None = None,
    filter: Expr | None = None,
) -> Expr:
    """Returns the value that is approximately at a given percentile of ``expr``.

    This aggregate function assumes the input values form a continuous distribution.
    Suppose you have a DataFrame which consists of 100 different test scores. If you
    called this function with a percentile of 0.9, it would return the value of the
    test score that is above 90% of the other test scores. The returned value may be
    between two of the values.

    This function uses the [t-digest](https://arxiv.org/abs/1902.04023) algorithm to
    compute the percentile. You can limit the number of bins used in this algorithm by
    setting the ``num_centroids`` parameter.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        sort_expression: Values for which to find the approximate percentile
        percentile: This must be between 0.0 and 1.0, inclusive
        num_centroids: Max bin size for the t-digest algorithm
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.0, 2.0, 3.0, 4.0, 5.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.approx_percentile_cont(
        ...         dfn.col("a"), 0.5
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        3.0

        >>> result = df.aggregate(
        ...     [], [dfn.functions.approx_percentile_cont(
        ...         dfn.col("a"), 0.5,
        ...         num_centroids=10,
        ...         filter=dfn.col("a") > dfn.lit(1.0),
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        3.5
    """
    sort_expr_raw = sort_or_default(sort_expression)
    filter_raw = filter.expr if filter is not None else None
    return Expr(
        f.approx_percentile_cont(
            sort_expr_raw, percentile, num_centroids=num_centroids, filter=filter_raw
        )
    )


def approx_percentile_cont_with_weight(
    sort_expression: Expr | SortExpr,
    weight: Expr,
    percentile: float,
    num_centroids: int | None = None,
    filter: Expr | None = None,
) -> Expr:
    """Returns the value of the weighted approximate percentile.

    This aggregate function is similar to :py:func:`approx_percentile_cont` except that
    it uses the associated associated weights.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        sort_expression: Values for which to find the approximate percentile
        weight: Relative weight for each of the values in ``expression``
        percentile: This must be between 0.0 and 1.0, inclusive
        num_centroids: Max bin size for the t-digest algorithm
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.0, 2.0, 3.0], "w": [1.0, 1.0, 1.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.approx_percentile_cont_with_weight(
        ...         dfn.col("a"), dfn.col("w"), 0.5
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2.0

        >>> result = df.aggregate(
        ...     [], [dfn.functions.approx_percentile_cont_with_weight(
        ...         dfn.col("a"), dfn.col("w"), 0.5,
        ...         num_centroids=10,
        ...         filter=dfn.col("a") > dfn.lit(1.0),
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2.5
    """
    sort_expr_raw = sort_or_default(sort_expression)
    filter_raw = filter.expr if filter is not None else None
    return Expr(
        f.approx_percentile_cont_with_weight(
            sort_expr_raw,
            weight.expr,
            percentile,
            num_centroids=num_centroids,
            filter=filter_raw,
        )
    )


def percentile_cont(
    sort_expression: Expr | SortExpr,
    percentile: float,
    filter: Expr | None = None,
) -> Expr:
    """Computes the exact percentile of input values using continuous interpolation.

    Unlike :py:func:`approx_percentile_cont`, this function computes the exact
    percentile value rather than an approximation.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        sort_expression: Values for which to find the percentile
        percentile: This must be between 0.0 and 1.0, inclusive
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.0, 2.0, 3.0, 4.0, 5.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.percentile_cont(
        ...         dfn.col("a"), 0.5
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        3.0

        >>> result = df.aggregate(
        ...     [], [dfn.functions.percentile_cont(
        ...         dfn.col("a"), 0.5,
        ...         filter=dfn.col("a") > dfn.lit(1.0),
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        3.5
    """
    sort_expr_raw = sort_or_default(sort_expression)
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.percentile_cont(sort_expr_raw, percentile, filter=filter_raw))


def quantile_cont(
    sort_expression: Expr | SortExpr,
    percentile: float,
    filter: Expr | None = None,
) -> Expr:
    """Computes the exact percentile of input values using continuous interpolation.

    See Also:
        This is an alias for :py:func:`percentile_cont`.
    """
    return percentile_cont(sort_expression, percentile, filter)


def array_agg(
    expression: Expr,
    distinct: bool = False,
    filter: Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
) -> Expr:
    """Aggregate values into an array.

    Currently ``distinct`` and ``order_by`` cannot be used together. As a work around,
    consider :py:func:`array_sort` after aggregation.
    [Issue Tracker](https://github.com/apache/datafusion/issues/12371)

    If using the builder functions described in ref:`_aggregation` this function ignores
    the option ``null_treatment``.

    Args:
        expression: Values to combine into an array
        distinct: If True, a single entry for each distinct value will be in the result
        filter: If provided, only compute against rows for which the filter is True
        order_by: Order the resultant array values. Accepts column names or expressions.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2, 3]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.array_agg(
        ...         dfn.col("a")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        [1, 2, 3]

        >>> df = ctx.from_pydict({"a": [3, 1, 2, 1]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.array_agg(
        ...         dfn.col("a"), distinct=True,
        ...     ).alias("v")])
        >>> sorted(result.collect_column("v")[0].as_py())
        [1, 2, 3]

        >>> result = df.aggregate(
        ...     [], [dfn.functions.array_agg(
        ...         dfn.col("a"),
        ...         filter=dfn.col("a") > dfn.lit(1),
        ...         order_by="a",
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        [2, 3]
    """
    order_by_raw = sort_list_to_raw_sort_list(order_by)
    filter_raw = filter.expr if filter is not None else None

    return Expr(
        f.array_agg(
            expression.expr, distinct=distinct, filter=filter_raw, order_by=order_by_raw
        )
    )


def grouping(
    expression: Expr,
    distinct: bool = False,
    filter: Expr | None = None,
) -> Expr:
    """Indicates whether a column is aggregated across in the current row.

    Returns 0 when the column is part of the grouping key for that row
    (i.e., the row contains per-group results for that column). Returns 1
    when the column is *not* part of the grouping key (i.e., the row's
    aggregate spans all values of that column).

    This function is meaningful with
    :py:meth:`GroupingSet.rollup <datafusion.expr.GroupingSet.rollup>`,
    :py:meth:`GroupingSet.cube <datafusion.expr.GroupingSet.cube>`, or
    :py:meth:`GroupingSet.grouping_sets <datafusion.expr.GroupingSet.grouping_sets>`,
    where different rows are grouped by different subsets of columns. In a
    default aggregation without grouping sets every column is always part
    of the key, so ``grouping()`` always returns 0.

    .. warning::

        Due to an upstream DataFusion limitation
        (`#21411 <https://github.com/apache/datafusion/issues/21411>`_),
        ``.alias()`` cannot be applied directly to a ``grouping()``
        expression. Doing so will raise an error at execution time. To
        rename the column, use
        :py:meth:`~datafusion.dataframe.DataFrame.with_column_renamed`
        on the result DataFrame instead.

    Args:
        expression: The column to check grouping status for
        distinct: If True, compute on distinct values only
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        With :py:meth:`~datafusion.expr.GroupingSet.rollup`, the result
        includes both per-group rows (``grouping(a) = 0``) and a
        grand-total row where ``a`` is aggregated across
        (``grouping(a) = 1``):

        >>> from datafusion.expr import GroupingSet
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 1, 2], "b": [10, 20, 30]})
        >>> result = df.aggregate(
        ...     [GroupingSet.rollup(dfn.col("a"))],
        ...     [dfn.functions.sum(dfn.col("b")).alias("s"),
        ...      dfn.functions.grouping(dfn.col("a"))],
        ... ).sort(dfn.col("a").sort(nulls_first=False))
        >>> result.collect_column("s").to_pylist()
        [30, 30, 60]

    See Also:
        :py:class:`~datafusion.expr.GroupingSet`
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.grouping(expression.expr, distinct=distinct, filter=filter_raw))


def avg(
    expression: Expr,
    filter: Expr | None = None,
) -> Expr:
    """Returns the average value.

    This aggregate function expects a numeric expression and will return a float.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: Values to combine into an array
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.0, 2.0, 3.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.avg(
        ...         dfn.col("a")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2.0

        >>> result = df.aggregate(
        ...     [], [dfn.functions.avg(
        ...         dfn.col("a"),
        ...         filter=dfn.col("a") > dfn.lit(1.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2.5
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.avg(expression.expr, filter=filter_raw))


def corr(value_y: Expr, value_x: Expr, filter: Expr | None = None) -> Expr:
    """Returns the correlation coefficient between ``value1`` and ``value2``.

    This aggregate function expects both values to be numeric and will return a float.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        value_y: The dependent variable for correlation
        value_x: The independent variable for correlation
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.0, 2.0, 3.0], "b": [1.0, 2.0, 3.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.corr(
        ...         dfn.col("a"), dfn.col("b")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        1.0

        >>> result = df.aggregate(
        ...     [], [dfn.functions.corr(
        ...         dfn.col("a"), dfn.col("b"),
        ...         filter=dfn.col("a") > dfn.lit(1.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        1.0
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.corr(value_y.expr, value_x.expr, filter=filter_raw))


def count(
    expressions: Expr | list[Expr] | None = None,
    distinct: bool = False,
    filter: Expr | None = None,
) -> Expr:
    """Returns the number of rows that match the given arguments.

    This aggregate function will count the non-null rows provided in the expression.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by`` and ``null_treatment``.

    Args:
        expressions: Argument to perform bitwise calculation on
        distinct: If True, a single entry for each distinct value will be in the result
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2, 3]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.count(
        ...         dfn.col("a")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        3

        >>> df = ctx.from_pydict({"a": [1, 1, 2, 3]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.count(
        ...         dfn.col("a"), distinct=True,
        ...         filter=dfn.col("a") > dfn.lit(1),
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2
    """
    filter_raw = filter.expr if filter is not None else None

    if expressions is None:
        args = [Expr.literal(1).expr]
    elif isinstance(expressions, list):
        args = [arg.expr for arg in expressions]
    else:
        args = [expressions.expr]

    return Expr(f.count(*args, distinct=distinct, filter=filter_raw))


def covar_pop(value_y: Expr, value_x: Expr, filter: Expr | None = None) -> Expr:
    """Computes the population covariance.

    This aggregate function expects both values to be numeric and will return a float.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        value_y: The dependent variable for covariance
        value_x: The independent variable for covariance
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.0, 5.0, 10.0], "b": [1.0, 2.0, 3.0]})
        >>> result = df.aggregate(
        ...     [],
        ...     [dfn.functions.covar_pop(
        ...         dfn.col("a"), dfn.col("b")
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        3.0

        >>> df = ctx.from_pydict(
        ...     {"a": [0.0, 1.0, 3.0], "b": [0.0, 1.0, 3.0]})
        >>> result = df.aggregate(
        ...     [],
        ...     [dfn.functions.covar_pop(
        ...         dfn.col("a"), dfn.col("b"),
        ...         filter=dfn.col("a") > dfn.lit(0.0)
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        1.0
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.covar_pop(value_y.expr, value_x.expr, filter=filter_raw))


def covar_samp(value_y: Expr, value_x: Expr, filter: Expr | None = None) -> Expr:
    """Computes the sample covariance.

    This aggregate function expects both values to be numeric and will return a float.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        value_y: The dependent variable for covariance
        value_x: The independent variable for covariance
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.covar_samp(
        ...         dfn.col("a"), dfn.col("b")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        1.0

        >>> result = df.aggregate(
        ...     [], [dfn.functions.covar_samp(
        ...         dfn.col("a"), dfn.col("b"),
        ...         filter=dfn.col("a") > dfn.lit(1.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        0.5
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.covar_samp(value_y.expr, value_x.expr, filter=filter_raw))


def covar(value_y: Expr, value_x: Expr, filter: Expr | None = None) -> Expr:
    """Computes the sample covariance.

    See Also:
        This is an alias for :py:func:`covar_samp`.
    """
    return covar_samp(value_y, value_x, filter)


def max(expression: Expr, filter: Expr | None = None) -> Expr:
    """Aggregate function that returns the maximum value of the argument.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: The value to find the maximum of
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2, 3]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.max(
        ...         dfn.col("a")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        3

        >>> result = df.aggregate(
        ...     [], [dfn.functions.max(
        ...         dfn.col("a"),
        ...         filter=dfn.col("a") < dfn.lit(3)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.max(expression.expr, filter=filter_raw))


def mean(expression: Expr, filter: Expr | None = None) -> Expr:
    """Returns the average (mean) value of the argument.

    See Also:
        This is an alias for :py:func:`avg`.
    """
    return avg(expression, filter)


def median(
    expression: Expr, distinct: bool = False, filter: Expr | None = None
) -> Expr:
    """Computes the median of a set of numbers.

    This aggregate function returns the median value of the expression for the given
    aggregate function.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by`` and ``null_treatment``.

    Args:
        expression: The value to compute the median of
        distinct: If True, a single entry for each distinct value will be in the result
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.0, 2.0, 3.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.median(
        ...         dfn.col("a")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2.0

        >>> df = ctx.from_pydict({"a": [1.0, 1.0, 2.0, 3.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.median(
        ...         dfn.col("a"), distinct=True,
        ...         filter=dfn.col("a") < dfn.lit(3.0),
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        1.5
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.median(expression.expr, distinct=distinct, filter=filter_raw))


def min(expression: Expr, filter: Expr | None = None) -> Expr:
    """Aggregate function that returns the minimum value of the argument.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: The value to find the minimum of
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2, 3]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.min(
        ...         dfn.col("a")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        1

        >>> result = df.aggregate(
        ...     [], [dfn.functions.min(
        ...         dfn.col("a"),
        ...         filter=dfn.col("a") > dfn.lit(1)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.min(expression.expr, filter=filter_raw))


def sum(
    expression: Expr,
    filter: Expr | None = None,
) -> Expr:
    """Computes the sum of a set of numbers.

    This aggregate function expects a numeric expression.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: Values to combine into an array
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2, 3]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.sum(
        ...         dfn.col("a")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        6

        >>> result = df.aggregate(
        ...     [], [dfn.functions.sum(
        ...         dfn.col("a"),
        ...         filter=dfn.col("a") > dfn.lit(1)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        5
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.sum(expression.expr, filter=filter_raw))


def stddev(expression: Expr, filter: Expr | None = None) -> Expr:
    """Computes the standard deviation of the argument.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: The value to find the minimum of
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [2.0, 4.0, 6.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.stddev(
        ...         dfn.col("a")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2.0

        >>> result = df.aggregate(
        ...     [], [dfn.functions.stddev(
        ...         dfn.col("a"),
        ...         filter=dfn.col("a") > dfn.lit(2.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        1.41...
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.stddev(expression.expr, filter=filter_raw))


def stddev_pop(expression: Expr, filter: Expr | None = None) -> Expr:
    """Computes the population standard deviation of the argument.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: The value to find the minimum of
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [0.0, 1.0, 3.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.stddev_pop(
        ...         dfn.col("a")
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        1.247...

        >>> df = ctx.from_pydict({"a": [0.0, 1.0, 3.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.stddev_pop(
        ...         dfn.col("a"),
        ...         filter=dfn.col("a") > dfn.lit(0.0)
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        1.0
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.stddev_pop(expression.expr, filter=filter_raw))


def stddev_samp(arg: Expr, filter: Expr | None = None) -> Expr:
    """Computes the sample standard deviation of the argument.

    See Also:
        This is an alias for :py:func:`stddev`.
    """
    return stddev(arg, filter=filter)


def var(expression: Expr, filter: Expr | None = None) -> Expr:
    """Computes the sample variance of the argument.

    See Also:
        This is an alias for :py:func:`var_samp`.
    """
    return var_samp(expression, filter)


def var_pop(expression: Expr, filter: Expr | None = None) -> Expr:
    """Computes the population variance of the argument.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: The variable to compute the variance for
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [-1.0, 0.0, 2.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.var_pop(
        ...         dfn.col("a")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        1.555...

        >>> result = df.aggregate(
        ...     [], [dfn.functions.var_pop(
        ...         dfn.col("a"),
        ...         filter=dfn.col("a") > dfn.lit(-1.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        1.0
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.var_pop(expression.expr, filter=filter_raw))


def var_population(expression: Expr, filter: Expr | None = None) -> Expr:
    """Computes the population variance of the argument.

    See Also:
        This is an alias for :py:func:`var_pop`.
    """
    return var_pop(expression, filter)


def var_samp(expression: Expr, filter: Expr | None = None) -> Expr:
    """Computes the sample variance of the argument.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: The variable to compute the variance for
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.0, 2.0, 3.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.var_samp(
        ...         dfn.col("a")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        1.0

        >>> result = df.aggregate(
        ...     [], [dfn.functions.var_samp(
        ...         dfn.col("a"),
        ...         filter=dfn.col("a") > dfn.lit(1.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        0.5
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.var_sample(expression.expr, filter=filter_raw))


def var_sample(expression: Expr, filter: Expr | None = None) -> Expr:
    """Computes the sample variance of the argument.

    See Also:
        This is an alias for :py:func:`var_samp`.
    """
    return var_samp(expression, filter)


def regr_avgx(
    y: Expr,
    x: Expr,
    filter: Expr | None = None,
) -> Expr:
    """Computes the average of the independent variable ``x``.

    This is a linear regression aggregate function. Only non-null pairs of the inputs
    are evaluated.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        y: The linear regression dependent variable
        x: The linear regression independent variable
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"y": [1.0, 2.0, 3.0], "x": [4.0, 5.0, 6.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_avgx(
        ...         dfn.col("y"), dfn.col("x")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        5.0

        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_avgx(
        ...         dfn.col("y"), dfn.col("x"),
        ...         filter=dfn.col("y") > dfn.lit(1.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        5.5
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_avgx(y.expr, x.expr, filter=filter_raw))


def regr_avgy(
    y: Expr,
    x: Expr,
    filter: Expr | None = None,
) -> Expr:
    """Computes the average of the dependent variable ``y``.

    This is a linear regression aggregate function. Only non-null pairs of the inputs
    are evaluated.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        y: The linear regression dependent variable
        x: The linear regression independent variable
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"y": [1.0, 2.0, 3.0], "x": [4.0, 5.0, 6.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_avgy(
        ...         dfn.col("y"), dfn.col("x")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2.0

        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_avgy(
        ...         dfn.col("y"), dfn.col("x"),
        ...         filter=dfn.col("y") > dfn.lit(1.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2.5
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_avgy(y.expr, x.expr, filter=filter_raw))


def regr_count(
    y: Expr,
    x: Expr,
    filter: Expr | None = None,
) -> Expr:
    """Counts the number of rows in which both expressions are not null.

    This is a linear regression aggregate function. Only non-null pairs of the inputs
    are evaluated.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        y: The linear regression dependent variable
        x: The linear regression independent variable
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"y": [1.0, 2.0, 3.0], "x": [4.0, 5.0, 6.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_count(
        ...         dfn.col("y"), dfn.col("x")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        3

        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_count(
        ...         dfn.col("y"), dfn.col("x"),
        ...         filter=dfn.col("y") > dfn.lit(1.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_count(y.expr, x.expr, filter=filter_raw))


def regr_intercept(
    y: Expr,
    x: Expr,
    filter: Expr | None = None,
) -> Expr:
    """Computes the intercept from the linear regression.

    This is a linear regression aggregate function. Only non-null pairs of the inputs
    are evaluated.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        y: The linear regression dependent variable
        x: The linear regression independent variable
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"y": [2.0, 4.0, 6.0], "x": [4.0, 16.0, 36.0]})
        >>> result = df.aggregate(
        ...     [],
        ...     [dfn.functions.regr_intercept(
        ...         dfn.col("y"), dfn.col("x")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        1.714...

        >>> result = df.aggregate(
        ...     [],
        ...     [dfn.functions.regr_intercept(
        ...         dfn.col("y"), dfn.col("x"),
        ...         filter=dfn.col("y") > dfn.lit(2.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2.4
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_intercept(y.expr, x.expr, filter=filter_raw))


def regr_r2(
    y: Expr,
    x: Expr,
    filter: Expr | None = None,
) -> Expr:
    """Computes the R-squared value from linear regression.

    This is a linear regression aggregate function. Only non-null pairs of the inputs
    are evaluated.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        y: The linear regression dependent variable
        x: The linear regression independent variable
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"y": [2.0, 4.0, 6.0], "x": [4.0, 16.0, 36.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_r2(
        ...         dfn.col("y"), dfn.col("x")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        0.9795...

        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_r2(
        ...         dfn.col("y"), dfn.col("x"),
        ...         filter=dfn.col("y") > dfn.lit(2.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        1.0
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_r2(y.expr, x.expr, filter=filter_raw))


def regr_slope(
    y: Expr,
    x: Expr,
    filter: Expr | None = None,
) -> Expr:
    """Computes the slope from linear regression.

    This is a linear regression aggregate function. Only non-null pairs of the inputs
    are evaluated.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        y: The linear regression dependent variable
        x: The linear regression independent variable
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"y": [2.0, 4.0, 6.0], "x": [4.0, 16.0, 36.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_slope(
        ...         dfn.col("y"), dfn.col("x")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        0.122...

        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_slope(
        ...         dfn.col("y"), dfn.col("x"),
        ...         filter=dfn.col("y") > dfn.lit(2.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        0.1
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_slope(y.expr, x.expr, filter=filter_raw))


def regr_sxx(
    y: Expr,
    x: Expr,
    filter: Expr | None = None,
) -> Expr:
    """Computes the sum of squares of the independent variable ``x``.

    This is a linear regression aggregate function. Only non-null pairs of the inputs
    are evaluated.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        y: The linear regression dependent variable
        x: The linear regression independent variable
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"y": [1.0, 2.0, 3.0], "x": [1.0, 2.0, 3.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_sxx(
        ...         dfn.col("y"), dfn.col("x")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2.0

        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_sxx(
        ...         dfn.col("y"), dfn.col("x"),
        ...         filter=dfn.col("y") > dfn.lit(1.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        0.5
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_sxx(y.expr, x.expr, filter=filter_raw))


def regr_sxy(
    y: Expr,
    x: Expr,
    filter: Expr | None = None,
) -> Expr:
    """Computes the sum of products of pairs of numbers.

    This is a linear regression aggregate function. Only non-null pairs of the inputs
    are evaluated.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        y: The linear regression dependent variable
        x: The linear regression independent variable
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"y": [1.0, 2.0, 3.0], "x": [1.0, 2.0, 3.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_sxy(
        ...         dfn.col("y"), dfn.col("x")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2.0

        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_sxy(
        ...         dfn.col("y"), dfn.col("x"),
        ...         filter=dfn.col("y") > dfn.lit(1.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        0.5
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_sxy(y.expr, x.expr, filter=filter_raw))


def regr_syy(
    y: Expr,
    x: Expr,
    filter: Expr | None = None,
) -> Expr:
    """Computes the sum of squares of the dependent variable ``y``.

    This is a linear regression aggregate function. Only non-null pairs of the inputs
    are evaluated.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        y: The linear regression dependent variable
        x: The linear regression independent variable
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"y": [1.0, 2.0, 3.0], "x": [1.0, 2.0, 3.0]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_syy(
        ...         dfn.col("y"), dfn.col("x")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        2.0

        >>> result = df.aggregate(
        ...     [], [dfn.functions.regr_syy(
        ...         dfn.col("y"), dfn.col("x"),
        ...         filter=dfn.col("y") > dfn.lit(1.0)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        0.5
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_syy(y.expr, x.expr, filter=filter_raw))


def first_value(
    expression: Expr,
    filter: Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
    null_treatment: NullTreatment = NullTreatment.RESPECT_NULLS,
) -> Expr:
    """Returns the first value in a group of values.

    This aggregate function will return the first value in the partition.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the option ``distinct``.

    Args:
        expression: Argument to perform bitwise calculation on
        filter: If provided, only compute against rows for which the filter is True
        order_by: Set the ordering of the expression to evaluate. Accepts
            column names or expressions.
        null_treatment: Assign whether to respect or ignore null values.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [10, 20, 30]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.first_value(
        ...         dfn.col("a")
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        10

        >>> df = ctx.from_pydict({"a": [None, 20, 10]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.first_value(
        ...         dfn.col("a"),
        ...         filter=dfn.col("a") > dfn.lit(10),
        ...         order_by="a",
        ...         null_treatment=dfn.common.NullTreatment.IGNORE_NULLS,
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        20
    """
    order_by_raw = sort_list_to_raw_sort_list(order_by)
    filter_raw = filter.expr if filter is not None else None

    return Expr(
        f.first_value(
            expression.expr,
            filter=filter_raw,
            order_by=order_by_raw,
            null_treatment=null_treatment.value,
        )
    )


def last_value(
    expression: Expr,
    filter: Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
    null_treatment: NullTreatment = NullTreatment.RESPECT_NULLS,
) -> Expr:
    """Returns the last value in a group of values.

    This aggregate function will return the last value in the partition.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the option ``distinct``.

    Args:
        expression: Argument to perform bitwise calculation on
        filter: If provided, only compute against rows for which the filter is True
        order_by: Set the ordering of the expression to evaluate. Accepts
            column names or expressions.
        null_treatment: Assign whether to respect or ignore null values.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [10, 20, 30]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.last_value(
        ...         dfn.col("a")
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        30

        >>> df = ctx.from_pydict({"a": [None, 20, 10]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.last_value(
        ...         dfn.col("a"),
        ...         filter=dfn.col("a") > dfn.lit(10),
        ...         order_by="a",
        ...         null_treatment=dfn.common.NullTreatment.IGNORE_NULLS,
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        20
    """
    order_by_raw = sort_list_to_raw_sort_list(order_by)
    filter_raw = filter.expr if filter is not None else None

    return Expr(
        f.last_value(
            expression.expr,
            filter=filter_raw,
            order_by=order_by_raw,
            null_treatment=null_treatment.value,
        )
    )


def nth_value(
    expression: Expr,
    n: int,
    filter: Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
    null_treatment: NullTreatment = NullTreatment.RESPECT_NULLS,
) -> Expr:
    """Returns the n-th value in a group of values.

    This aggregate function will return the n-th value in the partition.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the option ``distinct``.

    Args:
        expression: Argument to perform bitwise calculation on
        n: Index of value to return. Starts at 1.
        filter: If provided, only compute against rows for which the filter is True
        order_by: Set the ordering of the expression to evaluate. Accepts
            column names or expressions.
        null_treatment: Assign whether to respect or ignore null values.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [10, 20, 30]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.nth_value(
        ...         dfn.col("a"), 1
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        10

        >>> result = df.aggregate(
        ...     [], [dfn.functions.nth_value(
        ...         dfn.col("a"), 1,
        ...         filter=dfn.col("a") > dfn.lit(10),
        ...         order_by="a",
        ...         null_treatment=dfn.common.NullTreatment.IGNORE_NULLS,
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        20
    """
    order_by_raw = sort_list_to_raw_sort_list(order_by)
    filter_raw = filter.expr if filter is not None else None

    return Expr(
        f.nth_value(
            expression.expr,
            n,
            filter=filter_raw,
            order_by=order_by_raw,
            null_treatment=null_treatment.value,
        )
    )


def bit_and(expression: Expr, filter: Expr | None = None) -> Expr:
    """Computes the bitwise AND of the argument.

    This aggregate function will bitwise compare every value in the input partition.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: Argument to perform bitwise calculation on
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [7, 3]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.bit_and(
        ...         dfn.col("a")
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        3

        >>> df = ctx.from_pydict({"a": [7, 5, 3]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.bit_and(
        ...         dfn.col("a"),
        ...         filter=dfn.col("a") > dfn.lit(3)
        ...     ).alias("v")])
        >>> result.collect_column("v")[0].as_py()
        5
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.bit_and(expression.expr, filter=filter_raw))


def bit_or(expression: Expr, filter: Expr | None = None) -> Expr:
    """Computes the bitwise OR of the argument.

    This aggregate function will bitwise compare every value in the input partition.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: Argument to perform bitwise calculation on
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.bit_or(
        ...         dfn.col("a")
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        3

        >>> df = ctx.from_pydict({"a": [1, 2, 4]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.bit_or(
        ...         dfn.col("a"),
        ...         filter=dfn.col("a") > dfn.lit(1)
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        6
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.bit_or(expression.expr, filter=filter_raw))


def bit_xor(
    expression: Expr, distinct: bool = False, filter: Expr | None = None
) -> Expr:
    """Computes the bitwise XOR of the argument.

    This aggregate function will bitwise compare every value in the input partition.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by`` and ``null_treatment``.

    Args:
        expression: Argument to perform bitwise calculation on
        distinct: If True, evaluate each unique value of expression only once
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [5, 3]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.bit_xor(
        ...         dfn.col("a")
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        6

        >>> df = ctx.from_pydict({"a": [5, 5, 3]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.bit_xor(
        ...         dfn.col("a"), distinct=True,
        ...         filter=dfn.col("a") > dfn.lit(3),
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        5
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.bit_xor(expression.expr, distinct=distinct, filter=filter_raw))


def bool_and(expression: Expr, filter: Expr | None = None) -> Expr:
    """Computes the boolean AND of the argument.

    This aggregate function will compare every value in the input partition. These are
    expected to be boolean values.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: Argument to perform calculation on
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [True, True, False]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.bool_and(
        ...         dfn.col("a")
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        False

        >>> df = ctx.from_pydict(
        ...     {"a": [True, True, False], "b": [1, 2, 3]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.bool_and(
        ...         dfn.col("a"),
        ...         filter=dfn.col("b") < dfn.lit(3)
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.bool_and(expression.expr, filter=filter_raw))


def bool_or(expression: Expr, filter: Expr | None = None) -> Expr:
    """Computes the boolean OR of the argument.

    This aggregate function will compare every value in the input partition. These are
    expected to be boolean values.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: Argument to perform calculation on
        filter: If provided, only compute against rows for which the filter is True

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [False, False, True]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.bool_or(
        ...         dfn.col("a")
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        True

        >>> df = ctx.from_pydict(
        ...     {"a": [False, False, True], "b": [1, 2, 3]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.bool_or(
        ...         dfn.col("a"),
        ...         filter=dfn.col("b") < dfn.lit(3)
        ...     ).alias("v")]
        ... )
        >>> result.collect_column("v")[0].as_py()
        False
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.bool_or(expression.expr, filter=filter_raw))


def lead(
    arg: Expr,
    shift_offset: int = 1,
    default_value: Any | None = None,
    partition_by: list[Expr] | Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
) -> Expr:
    """Create a lead window function.

    Lead operation will return the argument that is in the next shift_offset-th row in
    the partition. For example ``lead(col("b"), shift_offset=3, default_value=5)`` will
    return the 3rd following value in column ``b``. At the end of the partition, where
    no further values can be returned it will return the default value of 5.

    Here is an example of both the ``lead`` and :py:func:`datafusion.functions.lag`
    functions on a simple DataFrame::

        +--------+------+-----+
        | points | lead | lag |
        +--------+------+-----+
        | 100    | 100  |     |
        | 100    | 50   | 100 |
        | 50     | 25   | 100 |
        | 25     |      | 50  |
        +--------+------+-----+

    To set window function parameters use the window builder approach described in the
    ref:`_window_functions` online documentation.

    Args:
        arg: Value to return
        shift_offset: Number of rows following the current row.
        default_value: Value to return if shift_offet row does not exist.
        partition_by: Expressions to partition the window frame on.
        order_by: Set ordering within the window frame. Accepts
            column names or expressions.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2, 3]})
        >>> result = df.select(
        ...     dfn.col("a"),
        ...     dfn.functions.lead(
        ...         dfn.col("a"), shift_offset=1,
        ...         default_value=0, order_by="a"
        ...     ).alias("lead"))
        >>> result.sort(dfn.col("a")).collect_column("lead").to_pylist()
        [2, 3, 0]

        >>> df = ctx.from_pydict({"g": ["a", "a", "b"], "v": [1, 2, 3]})
        >>> result = df.select(
        ...     dfn.col("g"), dfn.col("v"),
        ...     dfn.functions.lead(
        ...         dfn.col("v"), shift_offset=1, default_value=0,
        ...         partition_by=dfn.col("g"), order_by="v",
        ...     ).alias("lead"))
        >>> result.sort(dfn.col("g"), dfn.col("v")).collect_column("lead").to_pylist()
        [2, 0, 0]
    """
    if not isinstance(default_value, pa.Scalar) and default_value is not None:
        default_value = pa.scalar(default_value)

    partition_by_raw = expr_list_to_raw_expr_list(partition_by)
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.lead(
            arg.expr,
            shift_offset,
            default_value,
            partition_by=partition_by_raw,
            order_by=order_by_raw,
        )
    )


def lag(
    arg: Expr,
    shift_offset: int = 1,
    default_value: Any | None = None,
    partition_by: list[Expr] | Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
) -> Expr:
    """Create a lag window function.

    Lag operation will return the argument that is in the previous shift_offset-th row
    in the partition. For example ``lag(col("b"), shift_offset=3, default_value=5)``
    will return the 3rd previous value in column ``b``. At the beginning of the
    partition, where no values can be returned it will return the default value of 5.

    Here is an example of both the ``lag`` and :py:func:`datafusion.functions.lead`
    functions on a simple DataFrame::

        +--------+------+-----+
        | points | lead | lag |
        +--------+------+-----+
        | 100    | 100  |     |
        | 100    | 50   | 100 |
        | 50     | 25   | 100 |
        | 25     |      | 50  |
        +--------+------+-----+

    Args:
        arg: Value to return
        shift_offset: Number of rows before the current row.
        default_value: Value to return if shift_offet row does not exist.
        partition_by: Expressions to partition the window frame on.
        order_by: Set ordering within the window frame. Accepts
            column names or expressions.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2, 3]})
        >>> result = df.select(
        ...     dfn.col("a"),
        ...     dfn.functions.lag(
        ...         dfn.col("a"), shift_offset=1,
        ...         default_value=0, order_by="a"
        ...     ).alias("lag"))
        >>> result.sort(dfn.col("a")).collect_column("lag").to_pylist()
        [0, 1, 2]

        >>> df = ctx.from_pydict({"g": ["a", "a", "b"], "v": [1, 2, 3]})
        >>> result = df.select(
        ...     dfn.col("g"), dfn.col("v"),
        ...     dfn.functions.lag(
        ...         dfn.col("v"), shift_offset=1, default_value=0,
        ...         partition_by=dfn.col("g"), order_by="v",
        ...     ).alias("lag"))
        >>> result.sort(dfn.col("g"), dfn.col("v")).collect_column("lag").to_pylist()
        [0, 1, 0]
    """
    if not isinstance(default_value, pa.Scalar):
        default_value = pa.scalar(default_value)

    partition_by_raw = expr_list_to_raw_expr_list(partition_by)
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.lag(
            arg.expr,
            shift_offset,
            default_value,
            partition_by=partition_by_raw,
            order_by=order_by_raw,
        )
    )


def row_number(
    partition_by: list[Expr] | Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
) -> Expr:
    """Create a row number window function.

    Returns the row number of the window function.

    Here is an example of the ``row_number`` on a simple DataFrame::

        +--------+------------+
        | points | row number |
        +--------+------------+
        | 100    | 1          |
        | 100    | 2          |
        | 50     | 3          |
        | 25     | 4          |
        +--------+------------+

    Args:
        partition_by: Expressions to partition the window frame on.
        order_by: Set ordering within the window frame. Accepts
            column names or expressions.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [10, 20, 30]})
        >>> result = df.select(
        ...     dfn.col("a"),
        ...     dfn.functions.row_number(
        ...         order_by="a"
        ...     ).alias("rn"))
        >>> result.sort(dfn.col("a")).collect_column("rn").to_pylist()
        [1, 2, 3]

        >>> df = ctx.from_pydict(
        ...     {"g": ["a", "a", "b", "b"], "v": [1, 2, 3, 4]})
        >>> result = df.select(
        ...     dfn.col("g"), dfn.col("v"),
        ...     dfn.functions.row_number(
        ...         partition_by=dfn.col("g"), order_by="v",
        ...     ).alias("rn"))
        >>> result.sort(dfn.col("g"), dfn.col("v")).collect_column("rn").to_pylist()
        [1, 2, 1, 2]
    """
    partition_by_raw = expr_list_to_raw_expr_list(partition_by)
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.row_number(
            partition_by=partition_by_raw,
            order_by=order_by_raw,
        )
    )


def rank(
    partition_by: list[Expr] | Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
) -> Expr:
    """Create a rank window function.

    Returns the rank based upon the window order. Consecutive equal values will receive
    the same rank, but the next different value will not be consecutive but rather the
    number of rows that precede it plus one. This is similar to Olympic medals. If two
    people tie for gold, the next place is bronze. There would be no silver medal. Here
    is an example of a dataframe with a window ordered by descending ``points`` and the
    associated rank.

    You should set ``order_by`` to produce meaningful results::

        +--------+------+
        | points | rank |
        +--------+------+
        | 100    | 1    |
        | 100    | 1    |
        | 50     | 3    |
        | 25     | 4    |
        +--------+------+

    Args:
        partition_by: Expressions to partition the window frame on.
        order_by: Set ordering within the window frame. Accepts
            column names or expressions.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [10, 10, 20]})
        >>> result = df.select(
        ...     dfn.col("a"),
        ...     dfn.functions.rank(
        ...         order_by="a"
        ...     ).alias("rnk")
        ... )
        >>> result.sort(dfn.col("a")).collect_column("rnk").to_pylist()
        [1, 1, 3]

        >>> df = ctx.from_pydict(
        ...     {"g": ["a", "a", "b", "b"], "v": [1, 1, 2, 3]})
        >>> result = df.select(
        ...     dfn.col("g"), dfn.col("v"),
        ...     dfn.functions.rank(
        ...         partition_by=dfn.col("g"), order_by="v",
        ...     ).alias("rnk"))
        >>> result.sort(dfn.col("g"), dfn.col("v")).collect_column("rnk").to_pylist()
        [1, 1, 1, 2]
    """
    partition_by_raw = expr_list_to_raw_expr_list(partition_by)
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.rank(
            partition_by=partition_by_raw,
            order_by=order_by_raw,
        )
    )


def dense_rank(
    partition_by: list[Expr] | Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
) -> Expr:
    """Create a dense_rank window function.

    This window function is similar to :py:func:`rank` except that the returned values
    will be consecutive. Here is an example of a dataframe with a window ordered by
    descending ``points`` and the associated dense rank::

        +--------+------------+
        | points | dense_rank |
        +--------+------------+
        | 100    | 1          |
        | 100    | 1          |
        | 50     | 2          |
        | 25     | 3          |
        +--------+------------+

    Args:
        partition_by: Expressions to partition the window frame on.
        order_by: Set ordering within the window frame. Accepts
            column names or expressions.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [10, 10, 20]})
        >>> result = df.select(
        ...     dfn.col("a"),
        ...     dfn.functions.dense_rank(
        ...         order_by="a"
        ...     ).alias("dr"))
        >>> result.sort(dfn.col("a")).collect_column("dr").to_pylist()
        [1, 1, 2]

        >>> df = ctx.from_pydict(
        ...     {"g": ["a", "a", "b", "b"], "v": [1, 1, 2, 3]})
        >>> result = df.select(
        ...     dfn.col("g"), dfn.col("v"),
        ...     dfn.functions.dense_rank(
        ...         partition_by=dfn.col("g"), order_by="v",
        ...     ).alias("dr"))
        >>> result.sort(dfn.col("g"), dfn.col("v")).collect_column("dr").to_pylist()
        [1, 1, 1, 2]
    """
    partition_by_raw = expr_list_to_raw_expr_list(partition_by)
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.dense_rank(
            partition_by=partition_by_raw,
            order_by=order_by_raw,
        )
    )


def percent_rank(
    partition_by: list[Expr] | Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
) -> Expr:
    """Create a percent_rank window function.

    This window function is similar to :py:func:`rank` except that the returned values
    are the percentage from 0.0 to 1.0 from first to last. Here is an example of a
    dataframe with a window ordered by descending ``points`` and the associated percent
    rank::

        +--------+--------------+
        | points | percent_rank |
        +--------+--------------+
        | 100    | 0.0          |
        | 100    | 0.0          |
        | 50     | 0.666667     |
        | 25     | 1.0          |
        +--------+--------------+

    Args:
        partition_by: Expressions to partition the window frame on.
        order_by: Set ordering within the window frame. Accepts
            column names or expressions.


    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [10, 20, 30]})
        >>> result = df.select(
        ...     dfn.col("a"),
        ...     dfn.functions.percent_rank(
        ...         order_by="a"
        ...     ).alias("pr"))
        >>> result.sort(dfn.col("a")).collect_column("pr").to_pylist()
        [0.0, 0.5, 1.0]

        >>> df = ctx.from_pydict(
        ...     {"g": ["a", "a", "a", "b", "b"], "v": [1, 2, 3, 4, 5]})
        >>> result = df.select(
        ...     dfn.col("g"), dfn.col("v"),
        ...     dfn.functions.percent_rank(
        ...         partition_by=dfn.col("g"), order_by="v",
        ...     ).alias("pr"))
        >>> result.sort(dfn.col("g"), dfn.col("v")).collect_column("pr").to_pylist()
        [0.0, 0.5, 1.0, 0.0, 1.0]
    """
    partition_by_raw = expr_list_to_raw_expr_list(partition_by)
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.percent_rank(
            partition_by=partition_by_raw,
            order_by=order_by_raw,
        )
    )


def cume_dist(
    partition_by: list[Expr] | Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
) -> Expr:
    """Create a cumulative distribution window function.

    This window function is similar to :py:func:`rank` except that the returned values
    are the ratio of the row number to the total number of rows. Here is an example of a
    dataframe with a window ordered by descending ``points`` and the associated
    cumulative distribution::

        +--------+-----------+
        | points | cume_dist |
        +--------+-----------+
        | 100    | 0.5       |
        | 100    | 0.5       |
        | 50     | 0.75      |
        | 25     | 1.0       |
        +--------+-----------+

    Args:
        partition_by: Expressions to partition the window frame on.
        order_by: Set ordering within the window frame. Accepts
            column names or expressions.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1., 2., 2., 3.]})
        >>> result = df.select(
        ...     dfn.col("a"),
        ...     dfn.functions.cume_dist(
        ...         order_by="a"
        ...     ).alias("cd")
        ... )
        >>> result.collect_column("cd").to_pylist()
        [0.25..., 0.75..., 0.75..., 1.0...]

        >>> df = ctx.from_pydict(
        ...     {"g": ["a", "a", "b", "b"], "v": [1, 2, 3, 4]})
        >>> result = df.select(
        ...     dfn.col("g"), dfn.col("v"),
        ...     dfn.functions.cume_dist(
        ...         partition_by=dfn.col("g"), order_by="v",
        ...     ).alias("cd"))
        >>> result.sort(dfn.col("g"), dfn.col("v")).collect_column("cd").to_pylist()
        [0.5, 1.0, 0.5, 1.0]
    """
    partition_by_raw = expr_list_to_raw_expr_list(partition_by)
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.cume_dist(
            partition_by=partition_by_raw,
            order_by=order_by_raw,
        )
    )


def ntile(
    groups: int,
    partition_by: list[Expr] | Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
) -> Expr:
    """Create a n-tile window function.

    This window function orders the window frame into a give number of groups based on
    the ordering criteria. It then returns which group the current row is assigned to.
    Here is an example of a dataframe with a window ordered by descending ``points``
    and the associated n-tile function::

        +--------+-------+
        | points | ntile |
        +--------+-------+
        | 120    | 1     |
        | 100    | 1     |
        | 80     | 2     |
        | 60     | 2     |
        | 40     | 3     |
        | 20     | 3     |
        +--------+-------+

    Args:
        groups: Number of groups for the n-tile to be divided into.
        partition_by: Expressions to partition the window frame on.
        order_by: Set ordering within the window frame. Accepts
            column names or expressions.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [10, 20, 30, 40]})
        >>> result = df.select(
        ...     dfn.col("a"),
        ...     dfn.functions.ntile(
        ...         2, order_by="a"
        ...     ).alias("nt"))
        >>> result.sort(dfn.col("a")).collect_column("nt").to_pylist()
        [1, 1, 2, 2]

        >>> df = ctx.from_pydict(
        ...     {"g": ["a", "a", "b", "b"], "v": [1, 2, 3, 4]})
        >>> result = df.select(
        ...     dfn.col("g"), dfn.col("v"),
        ...     dfn.functions.ntile(
        ...         2, partition_by=dfn.col("g"), order_by="v",
        ...     ).alias("nt"))
        >>> result.sort(dfn.col("g"), dfn.col("v")).collect_column("nt").to_pylist()
        [1, 2, 1, 2]
    """
    partition_by_raw = expr_list_to_raw_expr_list(partition_by)
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.ntile(
            Expr.literal(groups).expr,
            partition_by=partition_by_raw,
            order_by=order_by_raw,
        )
    )


def string_agg(
    expression: Expr,
    delimiter: str,
    filter: Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
) -> Expr:
    """Concatenates the input strings.

    This aggregate function will concatenate input strings, ignoring null values, and
    separating them with the specified delimiter. Non-string values will be converted to
    their string equivalents.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``distinct`` and ``null_treatment``.

    Args:
        expression: Argument to perform bitwise calculation on
        delimiter: Text to place between each value of expression
        filter: If provided, only compute against rows for which the filter is True
        order_by: Set the ordering of the expression to evaluate. Accepts
            column names or expressions.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["x", "y", "z"]})
        >>> result = df.aggregate(
        ...     [], [dfn.functions.string_agg(
        ...         dfn.col("a"), ",", order_by="a"
        ...     ).alias("s")])
        >>> result.collect_column("s")[0].as_py()
        'x,y,z'

        >>> result = df.aggregate(
        ...     [], [dfn.functions.string_agg(
        ...         dfn.col("a"), ",",
        ...         filter=dfn.col("a") > dfn.lit("x"),
        ...         order_by="a",
        ...     ).alias("s")])
        >>> result.collect_column("s")[0].as_py()
        'y,z'
    """
    order_by_raw = sort_list_to_raw_sort_list(order_by)
    filter_raw = filter.expr if filter is not None else None

    return Expr(
        f.string_agg(
            expression.expr,
            delimiter,
            filter=filter_raw,
            order_by=order_by_raw,
        )
    )
