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

from typing import TYPE_CHECKING, Any, Optional

import pyarrow as pa

from datafusion._internal import functions as f
from datafusion.common import NullTreatment
from datafusion.expr import (
    CaseBuilder,
    Expr,
    SortExpr,
    WindowFrame,
    expr_list_to_raw_expr_list,
    sort_list_to_raw_sort_list,
)

if TYPE_CHECKING:
    from datafusion.context import SessionContext

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
    "array_append",
    "array_cat",
    "array_concat",
    "array_dims",
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
    "array_slice",
    "array_sort",
    "array_to_string",
    "array_union",
    "arrow_cast",
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
    "date_bin",
    "date_part",
    "date_trunc",
    "datepart",
    "datetrunc",
    "decode",
    "degrees",
    "dense_rank",
    "digest",
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
    "in_list",
    "initcap",
    "isnan",
    "iszero",
    "lag",
    "last_value",
    "lcm",
    "lead",
    "left",
    "length",
    "levenshtein",
    "list_append",
    "list_cat",
    "list_concat",
    "list_dims",
    "list_distinct",
    "list_element",
    "list_except",
    "list_extract",
    "list_indexof",
    "list_intersect",
    "list_join",
    "list_length",
    "list_ndims",
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
    "list_slice",
    "list_sort",
    "list_to_string",
    "list_union",
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
    "octet_length",
    "order_by",
    "overlay",
    "percent_rank",
    "pi",
    "pow",
    "power",
    "radians",
    "random",
    "range",
    "rank",
    "regexp_count",
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
    "strpos",
    "struct",
    "substr",
    "substr_index",
    "substring",
    "sum",
    "tan",
    "tanh",
    "to_hex",
    "to_timestamp",
    "to_timestamp_micros",
    "to_timestamp_millis",
    "to_timestamp_nanos",
    "to_timestamp_seconds",
    "to_unixtime",
    "translate",
    "trim",
    "trunc",
    "upper",
    "uuid",
    "var",
    "var_pop",
    "var_samp",
    "var_sample",
    "when",
    # Window Functions
    "window",
]


def isnan(expr: Expr) -> Expr:
    """Returns true if a given number is +NaN or -NaN otherwise returns false."""
    return Expr(f.isnan(expr.expr))


def nullif(expr1: Expr, expr2: Expr) -> Expr:
    """Returns NULL if expr1 equals expr2; otherwise it returns expr1.

    This can be used to perform the inverse operation of the COALESCE expression.
    """
    return Expr(f.nullif(expr1.expr, expr2.expr))


def encode(expr: Expr, encoding: Expr) -> Expr:
    """Encode the ``input``, using the ``encoding``. encoding can be base64 or hex."""
    return Expr(f.encode(expr.expr, encoding.expr))


def decode(expr: Expr, encoding: Expr) -> Expr:
    """Decode the ``input``, using the ``encoding``. encoding can be base64 or hex."""
    return Expr(f.decode(expr.expr, encoding.expr))


def array_to_string(expr: Expr, delimiter: Expr) -> Expr:
    """Converts each element to its text representation."""
    return Expr(f.array_to_string(expr.expr, delimiter.expr.cast(pa.string())))


def array_join(expr: Expr, delimiter: Expr) -> Expr:
    """Converts each element to its text representation.

    This is an alias for :py:func:`array_to_string`.
    """
    return array_to_string(expr, delimiter)


def list_to_string(expr: Expr, delimiter: Expr) -> Expr:
    """Converts each element to its text representation.

    This is an alias for :py:func:`array_to_string`.
    """
    return array_to_string(expr, delimiter)


def list_join(expr: Expr, delimiter: Expr) -> Expr:
    """Converts each element to its text representation.

    This is an alias for :py:func:`array_to_string`.
    """
    return array_to_string(expr, delimiter)


def in_list(arg: Expr, values: list[Expr], negated: bool = False) -> Expr:
    """Returns whether the argument is contained within the list ``values``."""
    values = [v.expr for v in values]
    return Expr(f.in_list(arg.expr, values, negated))


def digest(value: Expr, method: Expr) -> Expr:
    """Computes the binary hash of an expression using the specified algorithm.

    Standard algorithms are md5, sha224, sha256, sha384, sha512, blake2s,
    blake2b, and blake3.
    """
    return Expr(f.digest(value.expr, method.expr))


def concat(*args: Expr) -> Expr:
    """Concatenates the text representations of all the arguments.

    NULL arguments are ignored.
    """
    args = [arg.expr for arg in args]
    return Expr(f.concat(args))


def concat_ws(separator: str, *args: Expr) -> Expr:
    """Concatenates the list ``args`` with the separator.

    ``NULL`` arguments are ignored. ``separator`` should not be ``NULL``.
    """
    args = [arg.expr for arg in args]
    return Expr(f.concat_ws(separator, args))


def order_by(expr: Expr, ascending: bool = True, nulls_first: bool = True) -> SortExpr:
    """Creates a new sort expression."""
    return SortExpr(expr, ascending=ascending, nulls_first=nulls_first)


def alias(expr: Expr, name: str, metadata: Optional[dict[str, str]] = None) -> Expr:
    """Creates an alias expression with an optional metadata dictionary.

    Args:
        expr: The expression to alias
        name: The alias name
        metadata: Optional metadata to attach to the column

    Returns:
        An expression with the given alias
    """
    return Expr(f.alias(expr.expr, name, metadata))


def col(name: str) -> Expr:
    """Creates a column reference expression."""
    return Expr(f.col(name))


def count_star(filter: Optional[Expr] = None) -> Expr:
    """Create a COUNT(1) aggregate expression.

    This aggregate function will count all of the rows in the partition.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``distinct``, and ``null_treatment``.

    Args:
        filter: If provided, only count rows for which the filter is True
    """
    return count(Expr.literal(1), filter=filter)


def case(expr: Expr) -> CaseBuilder:
    """Create a case expression.

    Create a :py:class:`~datafusion.expr.CaseBuilder` to match cases for the
    expression ``expr``. See :py:class:`~datafusion.expr.CaseBuilder` for
    detailed usage.
    """
    return CaseBuilder(f.case(expr.expr))


def when(when: Expr, then: Expr) -> CaseBuilder:
    """Create a case expression that has no base expression.

    Create a :py:class:`~datafusion.expr.CaseBuilder` to match cases for the
    expression ``expr``. See :py:class:`~datafusion.expr.CaseBuilder` for
    detailed usage.
    """
    return CaseBuilder(f.when(when.expr, then.expr))


def window(
    name: str,
    args: list[Expr],
    partition_by: list[Expr] | None = None,
    order_by: list[Expr | SortExpr] | None = None,
    window_frame: WindowFrame | None = None,
    ctx: SessionContext | None = None,
) -> Expr:
    """Creates a new Window function expression.

    This interface will soon be deprecated. Instead of using this interface,
    users should call the window functions directly. For example, to perform a
    lag use::

        df.select(functions.lag(col("a")).partition_by(col("b")).build())
    """
    args = [a.expr for a in args]
    partition_by = expr_list_to_raw_expr_list(partition_by)
    order_by_raw = sort_list_to_raw_sort_list(order_by)
    window_frame = window_frame.window_frame if window_frame is not None else None
    ctx = ctx.ctx if ctx is not None else None
    return Expr(f.window(name, args, partition_by, order_by_raw, window_frame, ctx))


# scalar functions
def abs(arg: Expr) -> Expr:
    """Return the absolute value of a given number.

    Returns:
    --------
    Expr
        A new expression representing the absolute value of the input expression.
    """
    return Expr(f.abs(arg.expr))


def acos(arg: Expr) -> Expr:
    """Returns the arc cosine or inverse cosine of a number.

    Returns:
    --------
    Expr
        A new expression representing the arc cosine of the input expression.
    """
    return Expr(f.acos(arg.expr))


def acosh(arg: Expr) -> Expr:
    """Returns inverse hyperbolic cosine."""
    return Expr(f.acosh(arg.expr))


def ascii(arg: Expr) -> Expr:
    """Returns the numeric code of the first character of the argument."""
    return Expr(f.ascii(arg.expr))


def asin(arg: Expr) -> Expr:
    """Returns the arc sine or inverse sine of a number."""
    return Expr(f.asin(arg.expr))


def asinh(arg: Expr) -> Expr:
    """Returns inverse hyperbolic sine."""
    return Expr(f.asinh(arg.expr))


def atan(arg: Expr) -> Expr:
    """Returns inverse tangent of a number."""
    return Expr(f.atan(arg.expr))


def atanh(arg: Expr) -> Expr:
    """Returns inverse hyperbolic tangent."""
    return Expr(f.atanh(arg.expr))


def atan2(y: Expr, x: Expr) -> Expr:
    """Returns inverse tangent of a division given in the argument."""
    return Expr(f.atan2(y.expr, x.expr))


def bit_length(arg: Expr) -> Expr:
    """Returns the number of bits in the string argument."""
    return Expr(f.bit_length(arg.expr))


def btrim(arg: Expr) -> Expr:
    """Removes all characters, spaces by default, from both sides of a string."""
    return Expr(f.btrim(arg.expr))


def cbrt(arg: Expr) -> Expr:
    """Returns the cube root of a number."""
    return Expr(f.cbrt(arg.expr))


def ceil(arg: Expr) -> Expr:
    """Returns the nearest integer greater than or equal to argument."""
    return Expr(f.ceil(arg.expr))


def character_length(arg: Expr) -> Expr:
    """Returns the number of characters in the argument."""
    return Expr(f.character_length(arg.expr))


def length(string: Expr) -> Expr:
    """The number of characters in the ``string``."""
    return Expr(f.length(string.expr))


def char_length(string: Expr) -> Expr:
    """The number of characters in the ``string``."""
    return Expr(f.char_length(string.expr))


def chr(arg: Expr) -> Expr:
    """Converts the Unicode code point to a UTF8 character."""
    return Expr(f.chr(arg.expr))


def coalesce(*args: Expr) -> Expr:
    """Returns the value of the first expr in ``args`` which is not NULL."""
    args = [arg.expr for arg in args]
    return Expr(f.coalesce(*args))


def cos(arg: Expr) -> Expr:
    """Returns the cosine of the argument."""
    return Expr(f.cos(arg.expr))


def cosh(arg: Expr) -> Expr:
    """Returns the hyperbolic cosine of the argument."""
    return Expr(f.cosh(arg.expr))


def cot(arg: Expr) -> Expr:
    """Returns the cotangent of the argument."""
    return Expr(f.cot(arg.expr))


def degrees(arg: Expr) -> Expr:
    """Converts the argument from radians to degrees."""
    return Expr(f.degrees(arg.expr))


def ends_with(arg: Expr, suffix: Expr) -> Expr:
    """Returns true if the ``string`` ends with the ``suffix``, false otherwise."""
    return Expr(f.ends_with(arg.expr, suffix.expr))


def exp(arg: Expr) -> Expr:
    """Returns the exponential of the argument."""
    return Expr(f.exp(arg.expr))


def factorial(arg: Expr) -> Expr:
    """Returns the factorial of the argument."""
    return Expr(f.factorial(arg.expr))


def find_in_set(string: Expr, string_list: Expr) -> Expr:
    """Find a string in a list of strings.

    Returns a value in the range of 1 to N if the string is in the string list
    ``string_list`` consisting of N substrings.

    The string list is a string composed of substrings separated by ``,`` characters.
    """
    return Expr(f.find_in_set(string.expr, string_list.expr))


def floor(arg: Expr) -> Expr:
    """Returns the nearest integer less than or equal to the argument."""
    return Expr(f.floor(arg.expr))


def gcd(x: Expr, y: Expr) -> Expr:
    """Returns the greatest common divisor."""
    return Expr(f.gcd(x.expr, y.expr))


def initcap(string: Expr) -> Expr:
    """Set the initial letter of each word to capital.

    Converts the first letter of each word in ``string`` to uppercase and the remaining
    characters to lowercase.
    """
    return Expr(f.initcap(string.expr))


def instr(string: Expr, substring: Expr) -> Expr:
    """Finds the position from where the ``substring`` matches the ``string``.

    This is an alias for :py:func:`strpos`.
    """
    return strpos(string, substring)


def iszero(arg: Expr) -> Expr:
    """Returns true if a given number is +0.0 or -0.0 otherwise returns false."""
    return Expr(f.iszero(arg.expr))


def lcm(x: Expr, y: Expr) -> Expr:
    """Returns the least common multiple."""
    return Expr(f.lcm(x.expr, y.expr))


def left(string: Expr, n: Expr) -> Expr:
    """Returns the first ``n`` characters in the ``string``."""
    return Expr(f.left(string.expr, n.expr))


def levenshtein(string1: Expr, string2: Expr) -> Expr:
    """Returns the Levenshtein distance between the two given strings."""
    return Expr(f.levenshtein(string1.expr, string2.expr))


def ln(arg: Expr) -> Expr:
    """Returns the natural logarithm (base e) of the argument."""
    return Expr(f.ln(arg.expr))


def log(base: Expr, num: Expr) -> Expr:
    """Returns the logarithm of a number for a particular ``base``."""
    return Expr(f.log(base.expr, num.expr))


def log10(arg: Expr) -> Expr:
    """Base 10 logarithm of the argument."""
    return Expr(f.log10(arg.expr))


def log2(arg: Expr) -> Expr:
    """Base 2 logarithm of the argument."""
    return Expr(f.log2(arg.expr))


def lower(arg: Expr) -> Expr:
    """Converts a string to lowercase."""
    return Expr(f.lower(arg.expr))


def lpad(string: Expr, count: Expr, characters: Expr | None = None) -> Expr:
    """Add left padding to a string.

    Extends the string to length length by prepending the characters fill (a
    space by default). If the string is already longer than length then it is
    truncated (on the right).
    """
    characters = characters if characters is not None else Expr.literal(" ")
    return Expr(f.lpad(string.expr, count.expr, characters.expr))


def ltrim(arg: Expr) -> Expr:
    """Removes all characters, spaces by default, from the beginning of a string."""
    return Expr(f.ltrim(arg.expr))


def md5(arg: Expr) -> Expr:
    """Computes an MD5 128-bit checksum for a string expression."""
    return Expr(f.md5(arg.expr))


def nanvl(x: Expr, y: Expr) -> Expr:
    """Returns ``x`` if ``x`` is not ``NaN``. Otherwise returns ``y``."""
    return Expr(f.nanvl(x.expr, y.expr))


def nvl(x: Expr, y: Expr) -> Expr:
    """Returns ``x`` if ``x`` is not ``NULL``. Otherwise returns ``y``."""
    return Expr(f.nvl(x.expr, y.expr))


def octet_length(arg: Expr) -> Expr:
    """Returns the number of bytes of a string."""
    return Expr(f.octet_length(arg.expr))


def overlay(
    string: Expr, substring: Expr, start: Expr, length: Expr | None = None
) -> Expr:
    """Replace a substring with a new substring.

    Replace the substring of string that starts at the ``start``'th character and
    extends for ``length`` characters with new substring.
    """
    if length is None:
        return Expr(f.overlay(string.expr, substring.expr, start.expr))
    return Expr(f.overlay(string.expr, substring.expr, start.expr, length.expr))


def pi() -> Expr:
    """Returns an approximate value of π."""
    return Expr(f.pi())


def position(string: Expr, substring: Expr) -> Expr:
    """Finds the position from where the ``substring`` matches the ``string``.

    This is an alias for :py:func:`strpos`.
    """
    return strpos(string, substring)


def power(base: Expr, exponent: Expr) -> Expr:
    """Returns ``base`` raised to the power of ``exponent``."""
    return Expr(f.power(base.expr, exponent.expr))


def pow(base: Expr, exponent: Expr) -> Expr:
    """Returns ``base`` raised to the power of ``exponent``.

    This is an alias of :py:func:`power`.
    """
    return power(base, exponent)


def radians(arg: Expr) -> Expr:
    """Converts the argument from degrees to radians."""
    return Expr(f.radians(arg.expr))


def regexp_like(string: Expr, regex: Expr, flags: Expr | None = None) -> Expr:
    """Find if any regular expression (regex) matches exist.

    Tests a string using a regular expression returning true if at least one match,
    false otherwise.
    """
    if flags is not None:
        flags = flags.expr
    return Expr(f.regexp_like(string.expr, regex.expr, flags))


def regexp_match(string: Expr, regex: Expr, flags: Expr | None = None) -> Expr:
    """Perform regular expression (regex) matching.

    Returns an array with each element containing the leftmost-first match of the
    corresponding index in ``regex`` to string in ``string``.
    """
    if flags is not None:
        flags = flags.expr
    return Expr(f.regexp_match(string.expr, regex.expr, flags))


def regexp_replace(
    string: Expr, pattern: Expr, replacement: Expr, flags: Expr | None = None
) -> Expr:
    """Replaces substring(s) matching a PCRE-like regular expression.

    The full list of supported features and syntax can be found at
    <https://docs.rs/regex/latest/regex/#syntax>

    Supported flags with the addition of 'g' can be found at
    <https://docs.rs/regex/latest/regex/#grouping-and-flags>
    """
    if flags is not None:
        flags = flags.expr
    return Expr(f.regexp_replace(string.expr, pattern.expr, replacement.expr, flags))


def regexp_count(
    string: Expr, pattern: Expr, start: Expr, flags: Expr | None = None
) -> Expr:
    """Returns the number of matches in a string.

    Optional start position (the first position is 1) to search for the regular
    expression.
    """
    if flags is not None:
        flags = flags.expr
    start = start.expr if start is not None else Expr.expr
    return Expr(f.regexp_count(string.expr, pattern.expr, start, flags))


def repeat(string: Expr, n: Expr) -> Expr:
    """Repeats the ``string`` to ``n`` times."""
    return Expr(f.repeat(string.expr, n.expr))


def replace(string: Expr, from_val: Expr, to_val: Expr) -> Expr:
    """Replaces all occurrences of ``from_val`` with ``to_val`` in the ``string``."""
    return Expr(f.replace(string.expr, from_val.expr, to_val.expr))


def reverse(arg: Expr) -> Expr:
    """Reverse the string argument."""
    return Expr(f.reverse(arg.expr))


def right(string: Expr, n: Expr) -> Expr:
    """Returns the last ``n`` characters in the ``string``."""
    return Expr(f.right(string.expr, n.expr))


def round(value: Expr, decimal_places: Expr | None = None) -> Expr:
    """Round the argument to the nearest integer.

    If the optional ``decimal_places`` is specified, round to the nearest number of
    decimal places. You can specify a negative number of decimal places. For example
    ``round(lit(125.2345), lit(-2))`` would yield a value of ``100.0``.
    """
    if decimal_places is None:
        decimal_places = Expr.literal(0)
    return Expr(f.round(value.expr, decimal_places.expr))


def rpad(string: Expr, count: Expr, characters: Expr | None = None) -> Expr:
    """Add right padding to a string.

    Extends the string to length length by appending the characters fill (a space
    by default). If the string is already longer than length then it is truncated.
    """
    characters = characters if characters is not None else Expr.literal(" ")
    return Expr(f.rpad(string.expr, count.expr, characters.expr))


def rtrim(arg: Expr) -> Expr:
    """Removes all characters, spaces by default, from the end of a string."""
    return Expr(f.rtrim(arg.expr))


def sha224(arg: Expr) -> Expr:
    """Computes the SHA-224 hash of a binary string."""
    return Expr(f.sha224(arg.expr))


def sha256(arg: Expr) -> Expr:
    """Computes the SHA-256 hash of a binary string."""
    return Expr(f.sha256(arg.expr))


def sha384(arg: Expr) -> Expr:
    """Computes the SHA-384 hash of a binary string."""
    return Expr(f.sha384(arg.expr))


def sha512(arg: Expr) -> Expr:
    """Computes the SHA-512 hash of a binary string."""
    return Expr(f.sha512(arg.expr))


def signum(arg: Expr) -> Expr:
    """Returns the sign of the argument (-1, 0, +1)."""
    return Expr(f.signum(arg.expr))


def sin(arg: Expr) -> Expr:
    """Returns the sine of the argument."""
    return Expr(f.sin(arg.expr))


def sinh(arg: Expr) -> Expr:
    """Returns the hyperbolic sine of the argument."""
    return Expr(f.sinh(arg.expr))


def split_part(string: Expr, delimiter: Expr, index: Expr) -> Expr:
    """Split a string and return one part.

    Splits a string based on a delimiter and picks out the desired field based
    on the index.
    """
    return Expr(f.split_part(string.expr, delimiter.expr, index.expr))


def sqrt(arg: Expr) -> Expr:
    """Returns the square root of the argument."""
    return Expr(f.sqrt(arg.expr))


def starts_with(string: Expr, prefix: Expr) -> Expr:
    """Returns true if string starts with prefix."""
    return Expr(f.starts_with(string.expr, prefix.expr))


def strpos(string: Expr, substring: Expr) -> Expr:
    """Finds the position from where the ``substring`` matches the ``string``."""
    return Expr(f.strpos(string.expr, substring.expr))


def substr(string: Expr, position: Expr) -> Expr:
    """Substring from the ``position`` to the end."""
    return Expr(f.substr(string.expr, position.expr))


def substr_index(string: Expr, delimiter: Expr, count: Expr) -> Expr:
    """Returns an indexed substring.

    The return will be the ``string`` from before ``count`` occurrences of
    ``delimiter``.
    """
    return Expr(f.substr_index(string.expr, delimiter.expr, count.expr))


def substring(string: Expr, position: Expr, length: Expr) -> Expr:
    """Substring from the ``position`` with ``length`` characters."""
    return Expr(f.substring(string.expr, position.expr, length.expr))


def tan(arg: Expr) -> Expr:
    """Returns the tangent of the argument."""
    return Expr(f.tan(arg.expr))


def tanh(arg: Expr) -> Expr:
    """Returns the hyperbolic tangent of the argument."""
    return Expr(f.tanh(arg.expr))


def to_hex(arg: Expr) -> Expr:
    """Converts an integer to a hexadecimal string."""
    return Expr(f.to_hex(arg.expr))


def now() -> Expr:
    """Returns the current timestamp in nanoseconds.

    This will use the same value for all instances of now() in same statement.
    """
    return Expr(f.now())


def to_timestamp(arg: Expr, *formatters: Expr) -> Expr:
    """Converts a string and optional formats to a ``Timestamp`` in nanoseconds.

    For usage of ``formatters`` see the rust chrono package ``strftime`` package.

    [Documentation here.](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
    """
    if formatters is None:
        return f.to_timestamp(arg.expr)

    formatters = [f.expr for f in formatters]
    return Expr(f.to_timestamp(arg.expr, *formatters))


def to_timestamp_millis(arg: Expr, *formatters: Expr) -> Expr:
    """Converts a string and optional formats to a ``Timestamp`` in milliseconds.

    See :py:func:`to_timestamp` for a description on how to use formatters.
    """
    formatters = [f.expr for f in formatters]
    return Expr(f.to_timestamp_millis(arg.expr, *formatters))


def to_timestamp_micros(arg: Expr, *formatters: Expr) -> Expr:
    """Converts a string and optional formats to a ``Timestamp`` in microseconds.

    See :py:func:`to_timestamp` for a description on how to use formatters.
    """
    formatters = [f.expr for f in formatters]
    return Expr(f.to_timestamp_micros(arg.expr, *formatters))


def to_timestamp_nanos(arg: Expr, *formatters: Expr) -> Expr:
    """Converts a string and optional formats to a ``Timestamp`` in nanoseconds.

    See :py:func:`to_timestamp` for a description on how to use formatters.
    """
    formatters = [f.expr for f in formatters]
    return Expr(f.to_timestamp_nanos(arg.expr, *formatters))


def to_timestamp_seconds(arg: Expr, *formatters: Expr) -> Expr:
    """Converts a string and optional formats to a ``Timestamp`` in seconds.

    See :py:func:`to_timestamp` for a description on how to use formatters.
    """
    formatters = [f.expr for f in formatters]
    return Expr(f.to_timestamp_seconds(arg.expr, *formatters))


def to_unixtime(string: Expr, *format_arguments: Expr) -> Expr:
    """Converts a string and optional formats to a Unixtime."""
    args = [f.expr for f in format_arguments]
    return Expr(f.to_unixtime(string.expr, *args))


def current_date() -> Expr:
    """Returns current UTC date as a Date32 value."""
    return Expr(f.current_date())


def current_time() -> Expr:
    """Returns current UTC time as a Time64 value."""
    return Expr(f.current_time())


def datepart(part: Expr, date: Expr) -> Expr:
    """Return a specified part of a date.

    This is an alias for :py:func:`date_part`.
    """
    return date_part(part, date)


def date_part(part: Expr, date: Expr) -> Expr:
    """Extracts a subfield from the date."""
    return Expr(f.date_part(part.expr, date.expr))


def extract(part: Expr, date: Expr) -> Expr:
    """Extracts a subfield from the date.

    This is an alias for :py:func:`date_part`.
    """
    return date_part(part, date)


def date_trunc(part: Expr, date: Expr) -> Expr:
    """Truncates the date to a specified level of precision."""
    return Expr(f.date_trunc(part.expr, date.expr))


def datetrunc(part: Expr, date: Expr) -> Expr:
    """Truncates the date to a specified level of precision.

    This is an alias for :py:func:`date_trunc`.
    """
    return date_trunc(part, date)


def date_bin(stride: Expr, source: Expr, origin: Expr) -> Expr:
    """Coerces an arbitrary timestamp to the start of the nearest specified interval."""
    return Expr(f.date_bin(stride.expr, source.expr, origin.expr))


def make_date(year: Expr, month: Expr, day: Expr) -> Expr:
    """Make a date from year, month and day component parts."""
    return Expr(f.make_date(year.expr, month.expr, day.expr))


def translate(string: Expr, from_val: Expr, to_val: Expr) -> Expr:
    """Replaces the characters in ``from_val`` with the counterpart in ``to_val``."""
    return Expr(f.translate(string.expr, from_val.expr, to_val.expr))


def trim(arg: Expr) -> Expr:
    """Removes all characters, spaces by default, from both sides of a string."""
    return Expr(f.trim(arg.expr))


def trunc(num: Expr, precision: Expr | None = None) -> Expr:
    """Truncate the number toward zero with optional precision."""
    if precision is not None:
        return Expr(f.trunc(num.expr, precision.expr))
    return Expr(f.trunc(num.expr))


def upper(arg: Expr) -> Expr:
    """Converts a string to uppercase."""
    return Expr(f.upper(arg.expr))


def make_array(*args: Expr) -> Expr:
    """Returns an array using the specified input expressions."""
    args = [arg.expr for arg in args]
    return Expr(f.make_array(args))


def make_list(*args: Expr) -> Expr:
    """Returns an array using the specified input expressions.

    This is an alias for :py:func:`make_array`.
    """
    return make_array(*args)


def array(*args: Expr) -> Expr:
    """Returns an array using the specified input expressions.

    This is an alias for :py:func:`make_array`.
    """
    return make_array(*args)


def range(start: Expr, stop: Expr, step: Expr) -> Expr:
    """Create a list of values in the range between start and stop."""
    return Expr(f.range(start.expr, stop.expr, step.expr))


def uuid() -> Expr:
    """Returns uuid v4 as a string value."""
    return Expr(f.uuid())


def struct(*args: Expr) -> Expr:
    """Returns a struct with the given arguments."""
    args = [arg.expr for arg in args]
    return Expr(f.struct(*args))


def named_struct(name_pairs: list[tuple[str, Expr]]) -> Expr:
    """Returns a struct with the given names and arguments pairs."""
    name_pair_exprs = [
        [Expr.literal(pa.scalar(pair[0], type=pa.string())), pair[1]]
        for pair in name_pairs
    ]

    # flatten
    name_pairs = [x.expr for xs in name_pair_exprs for x in xs]
    return Expr(f.named_struct(*name_pairs))


def from_unixtime(arg: Expr) -> Expr:
    """Converts an integer to RFC3339 timestamp format string."""
    return Expr(f.from_unixtime(arg.expr))


def arrow_typeof(arg: Expr) -> Expr:
    """Returns the Arrow type of the expression."""
    return Expr(f.arrow_typeof(arg.expr))


def arrow_cast(expr: Expr, data_type: Expr) -> Expr:
    """Casts an expression to a specified data type."""
    return Expr(f.arrow_cast(expr.expr, data_type.expr))


def random() -> Expr:
    """Returns a random value in the range ``0.0 <= x < 1.0``."""
    return Expr(f.random())


def array_append(array: Expr, element: Expr) -> Expr:
    """Appends an element to the end of an array."""
    return Expr(f.array_append(array.expr, element.expr))


def array_push_back(array: Expr, element: Expr) -> Expr:
    """Appends an element to the end of an array.

    This is an alias for :py:func:`array_append`.
    """
    return array_append(array, element)


def list_append(array: Expr, element: Expr) -> Expr:
    """Appends an element to the end of an array.

    This is an alias for :py:func:`array_append`.
    """
    return array_append(array, element)


def list_push_back(array: Expr, element: Expr) -> Expr:
    """Appends an element to the end of an array.

    This is an alias for :py:func:`array_append`.
    """
    return array_append(array, element)


def array_concat(*args: Expr) -> Expr:
    """Concatenates the input arrays."""
    args = [arg.expr for arg in args]
    return Expr(f.array_concat(args))


def array_cat(*args: Expr) -> Expr:
    """Concatenates the input arrays.

    This is an alias for :py:func:`array_concat`.
    """
    return array_concat(*args)


def array_dims(array: Expr) -> Expr:
    """Returns an array of the array's dimensions."""
    return Expr(f.array_dims(array.expr))


def array_distinct(array: Expr) -> Expr:
    """Returns distinct values from the array after removing duplicates."""
    return Expr(f.array_distinct(array.expr))


def list_cat(*args: Expr) -> Expr:
    """Concatenates the input arrays.

    This is an alias for :py:func:`array_concat`, :py:func:`array_cat`.
    """
    return array_concat(*args)


def list_concat(*args: Expr) -> Expr:
    """Concatenates the input arrays.

    This is an alias for :py:func:`array_concat`, :py:func:`array_cat`.
    """
    return array_concat(*args)


def list_distinct(array: Expr) -> Expr:
    """Returns distinct values from the array after removing duplicates.

    This is an alias for :py:func:`array_distinct`.
    """
    return array_distinct(array)


def list_dims(array: Expr) -> Expr:
    """Returns an array of the array's dimensions.

    This is an alias for :py:func:`array_dims`.
    """
    return array_dims(array)


def array_element(array: Expr, n: Expr) -> Expr:
    """Extracts the element with the index n from the array."""
    return Expr(f.array_element(array.expr, n.expr))


def array_empty(array: Expr) -> Expr:
    """Returns a boolean indicating whether the array is empty."""
    return Expr(f.array_empty(array.expr))


def array_extract(array: Expr, n: Expr) -> Expr:
    """Extracts the element with the index n from the array.

    This is an alias for :py:func:`array_element`.
    """
    return array_element(array, n)


def list_element(array: Expr, n: Expr) -> Expr:
    """Extracts the element with the index n from the array.

    This is an alias for :py:func:`array_element`.
    """
    return array_element(array, n)


def list_extract(array: Expr, n: Expr) -> Expr:
    """Extracts the element with the index n from the array.

    This is an alias for :py:func:`array_element`.
    """
    return array_element(array, n)


def array_length(array: Expr) -> Expr:
    """Returns the length of the array."""
    return Expr(f.array_length(array.expr))


def list_length(array: Expr) -> Expr:
    """Returns the length of the array.

    This is an alias for :py:func:`array_length`.
    """
    return array_length(array)


def array_has(first_array: Expr, second_array: Expr) -> Expr:
    """Returns true if the element appears in the first array, otherwise false."""
    return Expr(f.array_has(first_array.expr, second_array.expr))


def array_has_all(first_array: Expr, second_array: Expr) -> Expr:
    """Determines if there is complete overlap ``second_array`` in ``first_array``.

    Returns true if each element of the second array appears in the first array.
    Otherwise, it returns false.
    """
    return Expr(f.array_has_all(first_array.expr, second_array.expr))


def array_has_any(first_array: Expr, second_array: Expr) -> Expr:
    """Determine if there is an overlap between ``first_array`` and ``second_array``.

    Returns true if at least one element of the second array appears in the first
    array. Otherwise, it returns false.
    """
    return Expr(f.array_has_any(first_array.expr, second_array.expr))


def array_position(array: Expr, element: Expr, index: int | None = 1) -> Expr:
    """Return the position of the first occurrence of ``element`` in ``array``."""
    return Expr(f.array_position(array.expr, element.expr, index))


def array_indexof(array: Expr, element: Expr, index: int | None = 1) -> Expr:
    """Return the position of the first occurrence of ``element`` in ``array``.

    This is an alias for :py:func:`array_position`.
    """
    return array_position(array, element, index)


def list_position(array: Expr, element: Expr, index: int | None = 1) -> Expr:
    """Return the position of the first occurrence of ``element`` in ``array``.

    This is an alias for :py:func:`array_position`.
    """
    return array_position(array, element, index)


def list_indexof(array: Expr, element: Expr, index: int | None = 1) -> Expr:
    """Return the position of the first occurrence of ``element`` in ``array``.

    This is an alias for :py:func:`array_position`.
    """
    return array_position(array, element, index)


def array_positions(array: Expr, element: Expr) -> Expr:
    """Searches for an element in the array and returns all occurrences."""
    return Expr(f.array_positions(array.expr, element.expr))


def list_positions(array: Expr, element: Expr) -> Expr:
    """Searches for an element in the array and returns all occurrences.

    This is an alias for :py:func:`array_positions`.
    """
    return array_positions(array, element)


def array_ndims(array: Expr) -> Expr:
    """Returns the number of dimensions of the array."""
    return Expr(f.array_ndims(array.expr))


def list_ndims(array: Expr) -> Expr:
    """Returns the number of dimensions of the array.

    This is an alias for :py:func:`array_ndims`.
    """
    return array_ndims(array)


def array_prepend(element: Expr, array: Expr) -> Expr:
    """Prepends an element to the beginning of an array."""
    return Expr(f.array_prepend(element.expr, array.expr))


def array_push_front(element: Expr, array: Expr) -> Expr:
    """Prepends an element to the beginning of an array.

    This is an alias for :py:func:`array_prepend`.
    """
    return array_prepend(element, array)


def list_prepend(element: Expr, array: Expr) -> Expr:
    """Prepends an element to the beginning of an array.

    This is an alias for :py:func:`array_prepend`.
    """
    return array_prepend(element, array)


def list_push_front(element: Expr, array: Expr) -> Expr:
    """Prepends an element to the beginning of an array.

    This is an alias for :py:func:`array_prepend`.
    """
    return array_prepend(element, array)


def array_pop_back(array: Expr) -> Expr:
    """Returns the array without the last element."""
    return Expr(f.array_pop_back(array.expr))


def array_pop_front(array: Expr) -> Expr:
    """Returns the array without the first element."""
    return Expr(f.array_pop_front(array.expr))


def array_remove(array: Expr, element: Expr) -> Expr:
    """Removes the first element from the array equal to the given value."""
    return Expr(f.array_remove(array.expr, element.expr))


def list_remove(array: Expr, element: Expr) -> Expr:
    """Removes the first element from the array equal to the given value.

    This is an alias for :py:func:`array_remove`.
    """
    return array_remove(array, element)


def array_remove_n(array: Expr, element: Expr, max: Expr) -> Expr:
    """Removes the first ``max`` elements from the array equal to the given value."""
    return Expr(f.array_remove_n(array.expr, element.expr, max.expr))


def list_remove_n(array: Expr, element: Expr, max: Expr) -> Expr:
    """Removes the first ``max`` elements from the array equal to the given value.

    This is an alias for :py:func:`array_remove_n`.
    """
    return array_remove_n(array, element, max)


def array_remove_all(array: Expr, element: Expr) -> Expr:
    """Removes all elements from the array equal to the given value."""
    return Expr(f.array_remove_all(array.expr, element.expr))


def list_remove_all(array: Expr, element: Expr) -> Expr:
    """Removes all elements from the array equal to the given value.

    This is an alias for :py:func:`array_remove_all`.
    """
    return array_remove_all(array, element)


def array_repeat(element: Expr, count: Expr) -> Expr:
    """Returns an array containing ``element`` ``count`` times."""
    return Expr(f.array_repeat(element.expr, count.expr))


def list_repeat(element: Expr, count: Expr) -> Expr:
    """Returns an array containing ``element`` ``count`` times.

    This is an alias for :py:func:`array_repeat`.
    """
    return array_repeat(element, count)


def array_replace(array: Expr, from_val: Expr, to_val: Expr) -> Expr:
    """Replaces the first occurrence of ``from_val`` with ``to_val``."""
    return Expr(f.array_replace(array.expr, from_val.expr, to_val.expr))


def list_replace(array: Expr, from_val: Expr, to_val: Expr) -> Expr:
    """Replaces the first occurrence of ``from_val`` with ``to_val``.

    This is an alias for :py:func:`array_replace`.
    """
    return array_replace(array, from_val, to_val)


def array_replace_n(array: Expr, from_val: Expr, to_val: Expr, max: Expr) -> Expr:
    """Replace ``n`` occurrences of ``from_val`` with ``to_val``.

    Replaces the first ``max`` occurrences of the specified element with another
    specified element.
    """
    return Expr(f.array_replace_n(array.expr, from_val.expr, to_val.expr, max.expr))


def list_replace_n(array: Expr, from_val: Expr, to_val: Expr, max: Expr) -> Expr:
    """Replace ``n`` occurrences of ``from_val`` with ``to_val``.

    Replaces the first ``max`` occurrences of the specified element with another
    specified element.

    This is an alias for :py:func:`array_replace_n`.
    """
    return array_replace_n(array, from_val, to_val, max)


def array_replace_all(array: Expr, from_val: Expr, to_val: Expr) -> Expr:
    """Replaces all occurrences of ``from_val`` with ``to_val``."""
    return Expr(f.array_replace_all(array.expr, from_val.expr, to_val.expr))


def list_replace_all(array: Expr, from_val: Expr, to_val: Expr) -> Expr:
    """Replaces all occurrences of ``from_val`` with ``to_val``.

    This is an alias for :py:func:`array_replace_all`.
    """
    return array_replace_all(array, from_val, to_val)


def array_sort(array: Expr, descending: bool = False, null_first: bool = False) -> Expr:
    """Sort an array.

    Args:
        array: The input array to sort.
        descending: If True, sorts in descending order.
        null_first: If True, nulls will be returned at the beginning of the array.
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
    """This is an alias for :py:func:`array_sort`."""
    return array_sort(array, descending=descending, null_first=null_first)


def array_slice(
    array: Expr, begin: Expr, end: Expr, stride: Expr | None = None
) -> Expr:
    """Returns a slice of the array."""
    if stride is not None:
        stride = stride.expr
    return Expr(f.array_slice(array.expr, begin.expr, end.expr, stride))


def list_slice(array: Expr, begin: Expr, end: Expr, stride: Expr | None = None) -> Expr:
    """Returns a slice of the array.

    This is an alias for :py:func:`array_slice`.
    """
    return array_slice(array, begin, end, stride)


def array_intersect(array1: Expr, array2: Expr) -> Expr:
    """Returns the intersection of ``array1`` and ``array2``."""
    return Expr(f.array_intersect(array1.expr, array2.expr))


def list_intersect(array1: Expr, array2: Expr) -> Expr:
    """Returns an the intersection of ``array1`` and ``array2``.

    This is an alias for :py:func:`array_intersect`.
    """
    return array_intersect(array1, array2)


def array_union(array1: Expr, array2: Expr) -> Expr:
    """Returns an array of the elements in the union of array1 and array2.

    Duplicate rows will not be returned.
    """
    return Expr(f.array_union(array1.expr, array2.expr))


def list_union(array1: Expr, array2: Expr) -> Expr:
    """Returns an array of the elements in the union of array1 and array2.

    Duplicate rows will not be returned.

    This is an alias for :py:func:`array_union`.
    """
    return array_union(array1, array2)


def array_except(array1: Expr, array2: Expr) -> Expr:
    """Returns the elements that appear in ``array1`` but not in ``array2``."""
    return Expr(f.array_except(array1.expr, array2.expr))


def list_except(array1: Expr, array2: Expr) -> Expr:
    """Returns the elements that appear in ``array1`` but not in the ``array2``.

    This is an alias for :py:func:`array_except`.
    """
    return array_except(array1, array2)


def array_resize(array: Expr, size: Expr, value: Expr) -> Expr:
    """Returns an array with the specified size filled.

    If ``size`` is greater than the ``array`` length, the additional entries will
    be filled with the given ``value``.
    """
    return Expr(f.array_resize(array.expr, size.expr, value.expr))


def list_resize(array: Expr, size: Expr, value: Expr) -> Expr:
    """Returns an array with the specified size filled.

    If ``size`` is greater than the ``array`` length, the additional entries will be
    filled with the given ``value``. This is an alias for :py:func:`array_resize`.
    """
    return array_resize(array, size, value)


def flatten(array: Expr) -> Expr:
    """Flattens an array of arrays into a single array."""
    return Expr(f.flatten(array.expr))


def cardinality(array: Expr) -> Expr:
    """Returns the total number of elements in the array."""
    return Expr(f.cardinality(array.expr))


def empty(array: Expr) -> Expr:
    """This is an alias for :py:func:`array_empty`."""
    return array_empty(array)


# aggregate functions
def approx_distinct(
    expression: Expr,
    filter: Optional[Expr] = None,
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
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.approx_distinct(expression.expr, filter=filter_raw))


def approx_median(expression: Expr, filter: Optional[Expr] = None) -> Expr:
    """Returns the approximate median value.

    This aggregate function is similar to :py:func:`median`, but it will only
    approximate the median. It may return significantly faster for some DataFrames.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by`` and ``null_treatment``, and ``distinct``.

    Args:
        expression: Values to find the median for
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.approx_median(expression.expr, filter=filter_raw))


def approx_percentile_cont(
    expression: Expr,
    percentile: float,
    num_centroids: Optional[int] = None,
    filter: Optional[Expr] = None,
) -> Expr:
    """Returns the value that is approximately at a given percentile of ``expr``.

    This aggregate function assumes the input values form a continuous distribution.
    Suppose you have a DataFrame which consists of 100 different test scores. If you
    called this function with a percentile of 0.9, it would return the value of the
    test score that is above 90% of the other test scores. The returned value may be
    between two of the values.

    This function uses the [t-digest](https://arxiv.org/abs/1902.04023) algorithm to
    compute the percentil. You can limit the number of bins used in this algorithm by
    setting the ``num_centroids`` parameter.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: Values for which to find the approximate percentile
        percentile: This must be between 0.0 and 1.0, inclusive
        num_centroids: Max bin size for the t-digest algorithm
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(
        f.approx_percentile_cont(
            expression.expr, percentile, num_centroids=num_centroids, filter=filter_raw
        )
    )


def approx_percentile_cont_with_weight(
    expression: Expr, weight: Expr, percentile: float, filter: Optional[Expr] = None
) -> Expr:
    """Returns the value of the weighted approximate percentile.

    This aggregate function is similar to :py:func:`approx_percentile_cont` except that
    it uses the associated associated weights.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: Values for which to find the approximate percentile
        weight: Relative weight for each of the values in ``expression``
        percentile: This must be between 0.0 and 1.0, inclusive
        filter: If provided, only compute against rows for which the filter is True

    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(
        f.approx_percentile_cont_with_weight(
            expression.expr, weight.expr, percentile, filter=filter_raw
        )
    )


def array_agg(
    expression: Expr,
    distinct: bool = False,
    filter: Optional[Expr] = None,
    order_by: Optional[list[Expr | SortExpr]] = None,
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
        order_by: Order the resultant array values
    """
    order_by_raw = sort_list_to_raw_sort_list(order_by)
    filter_raw = filter.expr if filter is not None else None

    return Expr(
        f.array_agg(
            expression.expr, distinct=distinct, filter=filter_raw, order_by=order_by_raw
        )
    )


def avg(
    expression: Expr,
    filter: Optional[Expr] = None,
) -> Expr:
    """Returns the average value.

    This aggregate function expects a numeric expression and will return a float.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: Values to combine into an array
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.avg(expression.expr, filter=filter_raw))


def corr(value_y: Expr, value_x: Expr, filter: Optional[Expr] = None) -> Expr:
    """Returns the correlation coefficient between ``value1`` and ``value2``.

    This aggregate function expects both values to be numeric and will return a float.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        value_y: The dependent variable for correlation
        value_x: The independent variable for correlation
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.corr(value_y.expr, value_x.expr, filter=filter_raw))


def count(
    expressions: Expr | list[Expr] | None = None,
    distinct: bool = False,
    filter: Optional[Expr] = None,
) -> Expr:
    """Returns the number of rows that match the given arguments.

    This aggregate function will count the non-null rows provided in the expression.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by`` and ``null_treatment``.

    Args:
        expressions: Argument to perform bitwise calculation on
        distinct: If True, a single entry for each distinct value will be in the result
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None

    if expressions is None:
        args = [Expr.literal(1).expr]
    elif isinstance(expressions, list):
        args = [arg.expr for arg in expressions]
    else:
        args = [expressions.expr]

    return Expr(f.count(*args, distinct=distinct, filter=filter_raw))


def covar_pop(value_y: Expr, value_x: Expr, filter: Optional[Expr] = None) -> Expr:
    """Computes the population covariance.

    This aggregate function expects both values to be numeric and will return a float.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        value_y: The dependent variable for covariance
        value_x: The independent variable for covariance
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.covar_pop(value_y.expr, value_x.expr, filter=filter_raw))


def covar_samp(value_y: Expr, value_x: Expr, filter: Optional[Expr] = None) -> Expr:
    """Computes the sample covariance.

    This aggregate function expects both values to be numeric and will return a float.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        value_y: The dependent variable for covariance
        value_x: The independent variable for covariance
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.covar_samp(value_y.expr, value_x.expr, filter=filter_raw))


def covar(value_y: Expr, value_x: Expr, filter: Optional[Expr] = None) -> Expr:
    """Computes the sample covariance.

    This is an alias for :py:func:`covar_samp`.
    """
    return covar_samp(value_y, value_x, filter)


def max(expression: Expr, filter: Optional[Expr] = None) -> Expr:
    """Aggregate function that returns the maximum value of the argument.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: The value to find the maximum of
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.max(expression.expr, filter=filter_raw))


def mean(expression: Expr, filter: Optional[Expr] = None) -> Expr:
    """Returns the average (mean) value of the argument.

    This is an alias for :py:func:`avg`.
    """
    return avg(expression, filter)


def median(
    expression: Expr, distinct: bool = False, filter: Optional[Expr] = None
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
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.median(expression.expr, distinct=distinct, filter=filter_raw))


def min(expression: Expr, filter: Optional[Expr] = None) -> Expr:
    """Returns the minimum value of the argument.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: The value to find the minimum of
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.min(expression.expr, filter=filter_raw))


def sum(
    expression: Expr,
    filter: Optional[Expr] = None,
) -> Expr:
    """Computes the sum of a set of numbers.

    This aggregate function expects a numeric expression.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: Values to combine into an array
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.sum(expression.expr, filter=filter_raw))


def stddev(expression: Expr, filter: Optional[Expr] = None) -> Expr:
    """Computes the standard deviation of the argument.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: The value to find the minimum of
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.stddev(expression.expr, filter=filter_raw))


def stddev_pop(expression: Expr, filter: Optional[Expr] = None) -> Expr:
    """Computes the population standard deviation of the argument.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: The value to find the minimum of
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.stddev_pop(expression.expr, filter=filter_raw))


def stddev_samp(arg: Expr, filter: Optional[Expr] = None) -> Expr:
    """Computes the sample standard deviation of the argument.

    This is an alias for :py:func:`stddev`.
    """
    return stddev(arg, filter=filter)


def var(expression: Expr, filter: Optional[Expr] = None) -> Expr:
    """Computes the sample variance of the argument.

    This is an alias for :py:func:`var_samp`.
    """
    return var_samp(expression, filter)


def var_pop(expression: Expr, filter: Optional[Expr] = None) -> Expr:
    """Computes the population variance of the argument.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: The variable to compute the variance for
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.var_pop(expression.expr, filter=filter_raw))


def var_samp(expression: Expr, filter: Optional[Expr] = None) -> Expr:
    """Computes the sample variance of the argument.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: The variable to compute the variance for
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.var_sample(expression.expr, filter=filter_raw))


def var_sample(expression: Expr, filter: Optional[Expr] = None) -> Expr:
    """Computes the sample variance of the argument.

    This is an alias for :py:func:`var_samp`.
    """
    return var_samp(expression, filter)


def regr_avgx(
    y: Expr,
    x: Expr,
    filter: Optional[Expr] = None,
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
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_avgx(y.expr, x.expr, filter=filter_raw))


def regr_avgy(
    y: Expr,
    x: Expr,
    filter: Optional[Expr] = None,
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
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_avgy(y.expr, x.expr, filter=filter_raw))


def regr_count(
    y: Expr,
    x: Expr,
    filter: Optional[Expr] = None,
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
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_count(y.expr, x.expr, filter=filter_raw))


def regr_intercept(
    y: Expr,
    x: Expr,
    filter: Optional[Expr] = None,
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
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_intercept(y.expr, x.expr, filter=filter_raw))


def regr_r2(
    y: Expr,
    x: Expr,
    filter: Optional[Expr] = None,
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
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_r2(y.expr, x.expr, filter=filter_raw))


def regr_slope(
    y: Expr,
    x: Expr,
    filter: Optional[Expr] = None,
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
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_slope(y.expr, x.expr, filter=filter_raw))


def regr_sxx(
    y: Expr,
    x: Expr,
    filter: Optional[Expr] = None,
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
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_sxx(y.expr, x.expr, filter=filter_raw))


def regr_sxy(
    y: Expr,
    x: Expr,
    filter: Optional[Expr] = None,
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
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_sxy(y.expr, x.expr, filter=filter_raw))


def regr_syy(
    y: Expr,
    x: Expr,
    filter: Optional[Expr] = None,
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
    """
    filter_raw = filter.expr if filter is not None else None

    return Expr(f.regr_syy(y.expr, x.expr, filter=filter_raw))


def first_value(
    expression: Expr,
    filter: Optional[Expr] = None,
    order_by: Optional[list[Expr | SortExpr]] = None,
    null_treatment: NullTreatment = NullTreatment.RESPECT_NULLS,
) -> Expr:
    """Returns the first value in a group of values.

    This aggregate function will return the first value in the partition.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the option ``distinct``.

    Args:
        expression: Argument to perform bitwise calculation on
        filter: If provided, only compute against rows for which the filter is True
        order_by: Set the ordering of the expression to evaluate
        null_treatment: Assign whether to respect or ignore null values.
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
    filter: Optional[Expr] = None,
    order_by: Optional[list[Expr | SortExpr]] = None,
    null_treatment: NullTreatment = NullTreatment.RESPECT_NULLS,
) -> Expr:
    """Returns the last value in a group of values.

    This aggregate function will return the last value in the partition.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the option ``distinct``.

    Args:
        expression: Argument to perform bitwise calculation on
        filter: If provided, only compute against rows for which the filter is True
        order_by: Set the ordering of the expression to evaluate
        null_treatment: Assign whether to respect or ignore null values.
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
    filter: Optional[Expr] = None,
    order_by: Optional[list[Expr | SortExpr]] = None,
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
        order_by: Set the ordering of the expression to evaluate
        null_treatment: Assign whether to respect or ignore null values.
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


def bit_and(expression: Expr, filter: Optional[Expr] = None) -> Expr:
    """Computes the bitwise AND of the argument.

    This aggregate function will bitwise compare every value in the input partition.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: Argument to perform bitwise calculation on
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.bit_and(expression.expr, filter=filter_raw))


def bit_or(expression: Expr, filter: Optional[Expr] = None) -> Expr:
    """Computes the bitwise OR of the argument.

    This aggregate function will bitwise compare every value in the input partition.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: Argument to perform bitwise calculation on
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.bit_or(expression.expr, filter=filter_raw))


def bit_xor(
    expression: Expr, distinct: bool = False, filter: Optional[Expr] = None
) -> Expr:
    """Computes the bitwise XOR of the argument.

    This aggregate function will bitwise compare every value in the input partition.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by`` and ``null_treatment``.

    Args:
        expression: Argument to perform bitwise calculation on
        distinct: If True, evaluate each unique value of expression only once
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.bit_xor(expression.expr, distinct=distinct, filter=filter_raw))


def bool_and(expression: Expr, filter: Optional[Expr] = None) -> Expr:
    """Computes the boolean AND of the argument.

    This aggregate function will compare every value in the input partition. These are
    expected to be boolean values.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: Argument to perform calculation on
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.bool_and(expression.expr, filter=filter_raw))


def bool_or(expression: Expr, filter: Optional[Expr] = None) -> Expr:
    """Computes the boolean OR of the argument.

    This aggregate function will compare every value in the input partition. These are
    expected to be boolean values.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``order_by``, ``null_treatment``, and ``distinct``.

    Args:
        expression: Argument to perform calculation on
        filter: If provided, only compute against rows for which the filter is True
    """
    filter_raw = filter.expr if filter is not None else None
    return Expr(f.bool_or(expression.expr, filter=filter_raw))


def lead(
    arg: Expr,
    shift_offset: int = 1,
    default_value: Optional[Any] = None,
    partition_by: Optional[list[Expr]] = None,
    order_by: Optional[list[Expr | SortExpr]] = None,
) -> Expr:
    """Create a lead window function.

    Lead operation will return the argument that is in the next shift_offset-th row in
    the partition. For example ``lead(col("b"), shift_offset=3, default_value=5)`` will
    return the 3rd following value in column ``b``. At the end of the partition, where
    no futher values can be returned it will return the default value of 5.

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
        order_by: Set ordering within the window frame.
    """
    if not isinstance(default_value, pa.Scalar) and default_value is not None:
        default_value = pa.scalar(default_value)

    partition_cols = (
        [col.expr for col in partition_by] if partition_by is not None else None
    )
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.lead(
            arg.expr,
            shift_offset,
            default_value,
            partition_by=partition_cols,
            order_by=order_by_raw,
        )
    )


def lag(
    arg: Expr,
    shift_offset: int = 1,
    default_value: Optional[Any] = None,
    partition_by: Optional[list[Expr]] = None,
    order_by: Optional[list[Expr | SortExpr]] = None,
) -> Expr:
    """Create a lag window function.

    Lag operation will return the argument that is in the previous shift_offset-th row
    in the partition. For example ``lag(col("b"), shift_offset=3, default_value=5)``
    will return the 3rd previous value in column ``b``. At the beginnig of the
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
        order_by: Set ordering within the window frame.
    """
    if not isinstance(default_value, pa.Scalar):
        default_value = pa.scalar(default_value)

    partition_cols = (
        [col.expr for col in partition_by] if partition_by is not None else None
    )
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.lag(
            arg.expr,
            shift_offset,
            default_value,
            partition_by=partition_cols,
            order_by=order_by_raw,
        )
    )


def row_number(
    partition_by: Optional[list[Expr]] = None,
    order_by: Optional[list[Expr | SortExpr]] = None,
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
        order_by: Set ordering within the window frame.
    """
    partition_cols = (
        [col.expr for col in partition_by] if partition_by is not None else None
    )
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.row_number(
            partition_by=partition_cols,
            order_by=order_by_raw,
        )
    )


def rank(
    partition_by: Optional[list[Expr]] = None,
    order_by: Optional[list[Expr | SortExpr]] = None,
) -> Expr:
    """Create a rank window function.

    Returns the rank based upon the window order. Consecutive equal values will receive
    the same rank, but the next different value will not be consecutive but rather the
    number of rows that preceed it plus one. This is similar to Olympic medals. If two
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
        order_by: Set ordering within the window frame.
    """
    partition_cols = (
        [col.expr for col in partition_by] if partition_by is not None else None
    )
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.rank(
            partition_by=partition_cols,
            order_by=order_by_raw,
        )
    )


def dense_rank(
    partition_by: Optional[list[Expr]] = None,
    order_by: Optional[list[Expr | SortExpr]] = None,
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
        order_by: Set ordering within the window frame.
    """
    partition_cols = (
        [col.expr for col in partition_by] if partition_by is not None else None
    )
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.dense_rank(
            partition_by=partition_cols,
            order_by=order_by_raw,
        )
    )


def percent_rank(
    partition_by: Optional[list[Expr]] = None,
    order_by: Optional[list[Expr | SortExpr]] = None,
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
        order_by: Set ordering within the window frame.
    """
    partition_cols = (
        [col.expr for col in partition_by] if partition_by is not None else None
    )
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.percent_rank(
            partition_by=partition_cols,
            order_by=order_by_raw,
        )
    )


def cume_dist(
    partition_by: Optional[list[Expr]] = None,
    order_by: Optional[list[Expr | SortExpr]] = None,
) -> Expr:
    """Create a cumulative distribution window function.

    This window function is similar to :py:func:`rank` except that the returned values
    are the ratio of the row number to the total numebr of rows. Here is an example of a
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
        order_by: Set ordering within the window frame.
    """
    partition_cols = (
        [col.expr for col in partition_by] if partition_by is not None else None
    )
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.cume_dist(
            partition_by=partition_cols,
            order_by=order_by_raw,
        )
    )


def ntile(
    groups: int,
    partition_by: Optional[list[Expr]] = None,
    order_by: Optional[list[Expr | SortExpr]] = None,
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
        order_by: Set ordering within the window frame.
    """
    partition_cols = (
        [col.expr for col in partition_by] if partition_by is not None else None
    )
    order_by_raw = sort_list_to_raw_sort_list(order_by)

    return Expr(
        f.ntile(
            Expr.literal(groups).expr,
            partition_by=partition_cols,
            order_by=order_by_raw,
        )
    )


def string_agg(
    expression: Expr,
    delimiter: str,
    filter: Optional[Expr] = None,
    order_by: Optional[list[Expr | SortExpr]] = None,
) -> Expr:
    """Concatenates the input strings.

    This aggregate function will concatenate input strings, ignoring null values, and
    seperating them with the specified delimiter. Non-string values will be converted to
    their string equivalents.

    If using the builder functions described in ref:`_aggregation` this function ignores
    the options ``distinct`` and ``null_treatment``.

    Args:
        expression: Argument to perform bitwise calculation on
        delimiter: Text to place between each value of expression
        filter: If provided, only compute against rows for which the filter is True
        order_by: Set the ordering of the expression to evaluate
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
