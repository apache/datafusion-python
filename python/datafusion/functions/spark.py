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

"""Spark-compatible function bindings.

These functions mirror the semantics of their Apache Spark counterparts
exactly. Some override DataFusion built-ins (``substring`` is 1-indexed,
``concat`` propagates NULL, ``round`` uses HALF_UP rounding, etc.), which is
why they live in a separate namespace rather than replacing the defaults.

For DataFrame use, import this module and call functions directly. For SQL
use, call :py:meth:`datafusion.SessionContext.enable_spark_functions` to
register the Spark UDFs by name (overriding any built-ins with matching
names) before issuing SQL queries.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa

from datafusion._internal import functions as _functions
from datafusion.expr import Expr, sort_list_to_raw_sort_list

if TYPE_CHECKING:
    from datafusion.common import NullTreatment
    from datafusion.expr import SortKey

_f = _functions.spark

# Reused int32 literal so optional-arg defaults don't rebuild it per call.
_ZERO_I32 = Expr.literal(pa.scalar(0, type=pa.int32()))


def _filter_raw(filter: Expr | None) -> Any:
    return filter.expr if filter is not None else None


# ---------------------------------------------------------------------------
# Aggregate functions
# ---------------------------------------------------------------------------


def avg(
    col: Expr,
    distinct: bool | None = None,
    filter: Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
    null_treatment: NullTreatment | None = None,
) -> Expr:
    """Spark ``avg``: returns the mean of a numeric column.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1.0, 2.0, 3.0]})
        >>> r = df.aggregate(
        ...     [], [dfn.functions.spark.avg(dfn.col("a")).alias("v")])
        >>> r.collect_column("v")[0].as_py()
        2.0
    """
    return Expr(
        _f.avg(
            col.expr,
            distinct=distinct,
            filter=_filter_raw(filter),
            order_by=sort_list_to_raw_sort_list(order_by),
            null_treatment=null_treatment,
        )
    )


def try_sum(
    col: Expr,
    distinct: bool | None = None,
    filter: Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
    null_treatment: NullTreatment | None = None,
) -> Expr:
    """Spark ``try_sum``: sum that returns NULL on overflow.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2, 3]})
        >>> r = df.aggregate(
        ...     [], [dfn.functions.spark.try_sum(dfn.col("a")).alias("v")])
        >>> r.collect_column("v")[0].as_py()
        6
    """
    return Expr(
        _f.try_sum(
            col.expr,
            distinct=distinct,
            filter=_filter_raw(filter),
            order_by=sort_list_to_raw_sort_list(order_by),
            null_treatment=null_treatment,
        )
    )


def collect_list(
    col: Expr,
    distinct: bool | None = None,
    filter: Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
    null_treatment: NullTreatment | None = None,
) -> Expr:
    """Spark ``collect_list``: collect values into an array (preserves dups).

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2, 2]})
        >>> r = df.aggregate(
        ...     [], [dfn.functions.spark.collect_list(dfn.col("a")).alias("v")])
        >>> sorted(r.collect_column("v")[0].as_py())
        [1, 2, 2]
    """
    return Expr(
        _f.collect_list(
            col.expr,
            distinct=distinct,
            filter=_filter_raw(filter),
            order_by=sort_list_to_raw_sort_list(order_by),
            null_treatment=null_treatment,
        )
    )


def collect_set(
    col: Expr,
    distinct: bool | None = None,
    filter: Expr | None = None,
    order_by: list[SortKey] | SortKey | None = None,
    null_treatment: NullTreatment | None = None,
) -> Expr:
    """Spark ``collect_set``: collect distinct values into an array.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2, 2, 3]})
        >>> r = df.aggregate(
        ...     [], [dfn.functions.spark.collect_set(dfn.col("a")).alias("v")])
        >>> sorted(r.collect_column("v")[0].as_py())
        [1, 2, 3]
    """
    return Expr(
        _f.collect_set(
            col.expr,
            distinct=distinct,
            filter=_filter_raw(filter),
            order_by=sort_list_to_raw_sort_list(order_by),
            null_treatment=null_treatment,
        )
    )


# ---------------------------------------------------------------------------
# Array functions
# ---------------------------------------------------------------------------


def array_contains(col: Expr, value: Expr) -> Expr:
    """Spark ``array_contains``: true if the array contains the element.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.array_contains(
        ...         dfn.functions.spark.array(dfn.lit(1), dfn.lit(2)),
        ...         dfn.lit(1),
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        True
    """
    return Expr(_f.array_contains(col.expr, value.expr))


def array(*cols: Expr) -> Expr:
    """Spark ``array``: builds an array from the given elements.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.array(
        ...         dfn.lit(1), dfn.lit(2), dfn.lit(3)
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        [1, 2, 3]
    """
    return Expr(_f.array(*[c.expr for c in cols]))


def shuffle(col: Expr, seed: int | None = None) -> Expr:
    """Spark ``shuffle``: returns a random permutation of the input array.

    ``seed`` is accepted for pyspark parity but is not yet wired through the
    Rust binding; passing a non-``None`` value raises ``NotImplementedError``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.shuffle(
        ...         dfn.functions.spark.array(dfn.lit(1), dfn.lit(2), dfn.lit(3))
        ...     ).alias("v")
        ... )
        >>> sorted(r.collect_column("v")[0].as_py())
        [1, 2, 3]
    """
    if seed is not None:
        msg = "shuffle(seed=...) is not yet supported by the Spark UDF binding"
        raise NotImplementedError(msg)
    return Expr(_f.shuffle(col.expr))


def array_repeat(col: Expr, count: Expr) -> Expr:
    """Spark ``array_repeat``: array of ``element`` repeated ``count`` times.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.array_repeat(dfn.lit("a"), dfn.lit(3)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        ['a', 'a', 'a']
    """
    return Expr(_f.array_repeat(col.expr, count.expr))


def slice(x: Expr, start: Expr, length: Expr) -> Expr:
    """Spark ``slice``: subset of the array from 1-indexed ``start`` with ``length``.

    Negative ``start`` counts from the end.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.slice(
        ...         dfn.functions.spark.array(
        ...             dfn.lit(1), dfn.lit(2), dfn.lit(3), dfn.lit(4)),
        ...         dfn.lit(2), dfn.lit(2),
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        [2, 3]
    """
    return Expr(_f.slice(x.expr, start.expr, length.expr))


# ---------------------------------------------------------------------------
# Bitmap functions
# ---------------------------------------------------------------------------


def bitmap_count(col: Expr) -> Expr:
    r"""Spark ``bitmap_count``: number of set bits in a bitmap.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.bitmap_count(dfn.lit(b"\xff")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        8
    """
    return Expr(_f.bitmap_count(col.expr))


def bitmap_bit_position(col: Expr) -> Expr:
    """Spark ``bitmap_bit_position``: bit position for a child expression.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.bitmap_bit_position(dfn.lit(15)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        14
    """
    return Expr(_f.bitmap_bit_position(col.expr))


def bitmap_bucket_number(col: Expr) -> Expr:
    """Spark ``bitmap_bucket_number``: bucket number for a child expression.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.bitmap_bucket_number(dfn.lit(15)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        1
    """
    return Expr(_f.bitmap_bucket_number(col.expr))


# ---------------------------------------------------------------------------
# Bitwise functions
# ---------------------------------------------------------------------------


def bit_get(col: Expr, pos: Expr) -> Expr:
    """Spark ``bit_get``: returns the bit (0 or 1) at ``pos``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.bit_get(dfn.lit(5), dfn.lit(0)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        1
    """
    return Expr(_f.bit_get(col.expr, pos.expr))


def bit_count(col: Expr) -> Expr:
    """Spark ``bit_count``: number of bits set in the integer's binary form.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.bit_count(dfn.lit(7)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        3
    """
    return Expr(_f.bit_count(col.expr))


def bitwise_not(col: Expr) -> Expr:
    """Spark ``~``: bitwise NOT.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.bitwise_not(dfn.lit(0)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        -1
    """
    return Expr(_f.bitwise_not(col.expr))


def shiftleft(col: Expr, numBits: Expr) -> Expr:  # noqa: N803
    """Spark ``shiftleft``: ``value`` shifted left by ``shift`` bits.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.shiftleft(dfn.lit(1), dfn.lit(3)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        8
    """
    return Expr(_f.shiftleft(col.expr, numBits.expr))


def shiftright(col: Expr, numBits: Expr) -> Expr:  # noqa: N803
    """Spark ``shiftright``: arithmetic right shift.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.shiftright(dfn.lit(8), dfn.lit(2)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        2
    """
    return Expr(_f.shiftright(col.expr, numBits.expr))


def shiftrightunsigned(col: Expr, numBits: Expr) -> Expr:  # noqa: N803
    """Spark ``shiftrightunsigned``: logical (unsigned) right shift.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.shiftrightunsigned(
        ...         dfn.lit(8), dfn.lit(2)
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        2
    """
    return Expr(_f.shiftrightunsigned(col.expr, numBits.expr))


# ---------------------------------------------------------------------------
# Collection / Conditional / Conversion
# ---------------------------------------------------------------------------


def size(col: Expr) -> Expr:
    """Spark ``size``: length of an array or map.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.size(
        ...         dfn.functions.spark.array(dfn.lit(1), dfn.lit(2), dfn.lit(3))
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        3
    """
    return Expr(_f.size(col.expr))


def if_(condition: Expr, if_true: Expr, if_false: Expr) -> Expr:
    """Spark ``if``: returns ``if_true`` when ``condition`` is true, else ``if_false``.

    Exposed as ``if_`` because ``if`` is a Python keyword.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": [1, 2]})
        >>> r = df.select(
        ...     dfn.functions.spark.if_(
        ...         dfn.col("a") > dfn.lit(1), dfn.lit("big"), dfn.lit("small")
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v").to_pylist()
        ['small', 'big']
    """
    return Expr(_f.if_(condition.expr, if_true.expr, if_false.expr))


def spark_cast(arg: Expr, type_str: Expr) -> Expr:
    """Spark ``cast``: cast ``arg`` to the type named by ``type_str``.

    Uses Spark cast semantics (e.g. overflow returns NULL, not error).

    Currently only supports casting numeric values to ``"timestamp"``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.spark_cast(
        ...         dfn.lit(1579098645), dfn.lit("timestamp")
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        datetime.datetime(2020, 1, 15, 14, 30, 45, tzinfo=<UTC>)
    """
    return Expr(_f.spark_cast(arg.expr, type_str.expr))


# ---------------------------------------------------------------------------
# Datetime functions
# ---------------------------------------------------------------------------


def add_months(start: Expr, months: Expr) -> Expr:
    """Spark ``add_months``: date + N months.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import date
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> d = dfn.lit(pa.scalar(date(2020, 1, 15), type=pa.date32()))
        >>> r = df.select(
        ...     dfn.functions.spark.add_months(
        ...         d, dfn.lit(pa.scalar(2, type=pa.int32()))
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        datetime.date(2020, 3, 15)
    """
    return Expr(_f.add_months(start.expr, months.expr))


def date_add(start: Expr, days: Expr) -> Expr:
    """Spark ``date_add``: date + N days.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import date
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> d = dfn.lit(pa.scalar(date(2020, 1, 15), type=pa.date32()))
        >>> r = df.select(
        ...     dfn.functions.spark.date_add(
        ...         d, dfn.lit(pa.scalar(5, type=pa.int32()))
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        datetime.date(2020, 1, 20)
    """
    return Expr(_f.date_add(start.expr, days.expr))


def date_sub(start: Expr, days: Expr) -> Expr:
    """Spark ``date_sub``: date - N days.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import date
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> d = dfn.lit(pa.scalar(date(2020, 1, 15), type=pa.date32()))
        >>> r = df.select(
        ...     dfn.functions.spark.date_sub(
        ...         d, dfn.lit(pa.scalar(5, type=pa.int32()))
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        datetime.date(2020, 1, 10)
    """
    return Expr(_f.date_sub(start.expr, days.expr))


def hour(col: Expr) -> Expr:
    """Spark ``hour``: extract hour component of a timestamp.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import datetime
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> ts = dfn.lit(
        ...     pa.scalar(datetime(2020, 1, 15, 14, 30, 45),
        ...               type=pa.timestamp('us')))
        >>> r = df.select(dfn.functions.spark.hour(ts).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        14
    """
    return Expr(_f.hour(col.expr))


def minute(col: Expr) -> Expr:
    """Spark ``minute``: extract minute component of a timestamp.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import datetime
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> ts = dfn.lit(
        ...     pa.scalar(datetime(2020, 1, 15, 14, 30, 45),
        ...               type=pa.timestamp('us')))
        >>> r = df.select(dfn.functions.spark.minute(ts).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        30
    """
    return Expr(_f.minute(col.expr))


def second(col: Expr) -> Expr:
    """Spark ``second``: extract second component of a timestamp.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import datetime
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> ts = dfn.lit(
        ...     pa.scalar(datetime(2020, 1, 15, 14, 30, 45),
        ...               type=pa.timestamp('us')))
        >>> r = df.select(dfn.functions.spark.second(ts).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        45
    """
    return Expr(_f.second(col.expr))


def last_day(col: Expr) -> Expr:
    """Spark ``last_day``: last day of the month containing the date.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import date
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> d = dfn.lit(pa.scalar(date(2020, 1, 15), type=pa.date32()))
        >>> r = df.select(dfn.functions.spark.last_day(d).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        datetime.date(2020, 1, 31)
    """
    return Expr(_f.last_day(col.expr))


def make_dt_interval(
    days: Expr | None = None,
    hours: Expr | None = None,
    mins: Expr | None = None,
    secs: Expr | None = None,
) -> Expr:
    """Spark ``make_dt_interval``: day-time interval from components.

    All parts are optional; omitted parts default to zero, matching pyspark.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.make_dt_interval().alias("v"))
        >>> r.collect_column("v")[0].as_py()
        datetime.timedelta(0)

        >>> import pyarrow as pa
        >>> i32 = lambda n: dfn.lit(pa.scalar(n, type=pa.int32()))
        >>> r = df.select(
        ...     dfn.functions.spark.make_dt_interval(
        ...         days=i32(1), hours=i32(2), mins=i32(3), secs=dfn.lit(4.5)
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        datetime.timedelta(days=1, seconds=7384, microseconds=500000)
    """
    return Expr(
        _f.make_dt_interval(
            (days if days is not None else _ZERO_I32).expr,
            (hours if hours is not None else _ZERO_I32).expr,
            (mins if mins is not None else _ZERO_I32).expr,
            (secs if secs is not None else Expr.literal(0.0)).expr,
        )
    )


def make_interval(
    years: Expr | None = None,
    months: Expr | None = None,
    weeks: Expr | None = None,
    days: Expr | None = None,
    hours: Expr | None = None,
    mins: Expr | None = None,
    secs: Expr | None = None,
) -> Expr:
    """Spark ``make_interval``: interval from year/month/week/day/hour/min/sec parts.

    All parts are optional; omitted parts default to zero, matching pyspark.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.make_interval().alias("v"))
        >>> r.collect_column("v")[0].as_py().months
        0

        >>> import pyarrow as pa
        >>> i32 = lambda n: dfn.lit(pa.scalar(n, type=pa.int32()))
        >>> r = df.select(dfn.functions.spark.make_interval(years=i32(1)).alias("v"))
        >>> r.collect_column("v")[0].as_py().months
        12
    """
    return Expr(
        _f.make_interval(
            (years if years is not None else _ZERO_I32).expr,
            (months if months is not None else _ZERO_I32).expr,
            (weeks if weeks is not None else _ZERO_I32).expr,
            (days if days is not None else _ZERO_I32).expr,
            (hours if hours is not None else _ZERO_I32).expr,
            (mins if mins is not None else _ZERO_I32).expr,
            (secs if secs is not None else Expr.literal(0.0)).expr,
        )
    )


def next_day(date: Expr, dayOfWeek: Expr) -> Expr:  # noqa: N803
    """Spark ``next_day``: first date after ``start_date`` named ``day_of_week``.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import date
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> d = dfn.lit(pa.scalar(date(2020, 1, 15), type=pa.date32()))
        >>> r = df.select(
        ...     dfn.functions.spark.next_day(d, dfn.lit("Mon")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        datetime.date(2020, 1, 20)
    """
    return Expr(_f.next_day(date.expr, dayOfWeek.expr))


def date_diff(end: Expr, start: Expr) -> Expr:
    """Spark ``date_diff``: number of days from ``start_date`` to ``end_date``.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import date
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> d = dfn.lit(pa.scalar(date(2020, 1, 15), type=pa.date32()))
        >>> end = dfn.lit(pa.scalar(date(2020, 1, 20), type=pa.date32()))
        >>> r = df.select(dfn.functions.spark.date_diff(end, d).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        5
    """
    return Expr(_f.date_diff(end.expr, start.expr))


def date_trunc(format: Expr, timestamp: Expr) -> Expr:
    """Spark ``date_trunc``: truncate timestamp to unit ``fmt``.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import datetime
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> ts = dfn.lit(
        ...     pa.scalar(datetime(2020, 1, 15, 14, 30, 45),
        ...               type=pa.timestamp('us')))
        >>> r = df.select(
        ...     dfn.functions.spark.date_trunc(dfn.lit("month"), ts).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        datetime.datetime(2020, 1, 1, 0, 0)
    """
    return Expr(_f.date_trunc(format.expr, timestamp.expr))


def time_trunc(unit: Expr, time: Expr) -> Expr:
    """Spark ``time_trunc``: truncate time value to unit ``fmt``.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import time
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> t = dfn.lit(pa.scalar(time(14, 30, 45), type=pa.time64('us')))
        >>> r = df.select(
        ...     dfn.functions.spark.time_trunc(dfn.lit("hour"), t).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        datetime.time(14, 0)
    """
    return Expr(_f.time_trunc(unit.expr, time.expr))


def trunc(date: Expr, format: Expr) -> Expr:
    """Spark ``trunc``: truncate date to unit ``fmt``.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import date
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> d = dfn.lit(pa.scalar(date(2020, 1, 15), type=pa.date32()))
        >>> r = df.select(
        ...     dfn.functions.spark.trunc(d, dfn.lit("YEAR")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        datetime.date(2020, 1, 1)
    """
    return Expr(_f.trunc(date.expr, format.expr))


def date_part(field: Expr, source: Expr) -> Expr:
    """Spark ``date_part``: extract ``field`` from a date/time/timestamp.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import date
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> d = dfn.lit(pa.scalar(date(2020, 1, 15), type=pa.date32()))
        >>> r = df.select(
        ...     dfn.functions.spark.date_part(dfn.lit("year"), d).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        2020
    """
    return Expr(_f.date_part(field.expr, source.expr))


def from_utc_timestamp(timestamp: Expr, tz: Expr) -> Expr:
    """Spark ``from_utc_timestamp``: interpret ``ts`` as UTC, convert to ``tz``.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import datetime
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> ts = dfn.lit(
        ...     pa.scalar(datetime(2020, 1, 15, 14, 30, 45),
        ...               type=pa.timestamp('us')))
        >>> r = df.select(
        ...     dfn.functions.spark.from_utc_timestamp(
        ...         ts, dfn.lit("UTC")
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        datetime.datetime(2020, 1, 15, 14, 30, 45)
    """
    return Expr(_f.from_utc_timestamp(timestamp.expr, tz.expr))


def to_utc_timestamp(timestamp: Expr, tz: Expr) -> Expr:
    """Spark ``to_utc_timestamp``: interpret ``ts`` as ``tz``, convert to UTC.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import datetime
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> ts = dfn.lit(
        ...     pa.scalar(datetime(2020, 1, 15, 14, 30, 45),
        ...               type=pa.timestamp('us')))
        >>> r = df.select(
        ...     dfn.functions.spark.to_utc_timestamp(
        ...         ts, dfn.lit("UTC")
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        datetime.datetime(2020, 1, 15, 14, 30, 45)
    """
    return Expr(_f.to_utc_timestamp(timestamp.expr, tz.expr))


def unix_date(col: Expr) -> Expr:
    """Spark ``unix_date``: days since 1970-01-01 for ``dt``.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import date
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> d = dfn.lit(pa.scalar(date(2020, 1, 15), type=pa.date32()))
        >>> r = df.select(dfn.functions.spark.unix_date(d).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        18276
    """
    return Expr(_f.unix_date(col.expr))


def unix_micros(col: Expr) -> Expr:
    """Spark ``unix_micros``: microseconds since epoch for ``ts``.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import datetime
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> ts = dfn.lit(
        ...     pa.scalar(datetime(2020, 1, 15, 14, 30, 45),
        ...               type=pa.timestamp('us')))
        >>> r = df.select(dfn.functions.spark.unix_micros(ts).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        1579098645000000
    """
    return Expr(_f.unix_micros(col.expr))


def unix_millis(col: Expr) -> Expr:
    """Spark ``unix_millis``: milliseconds since epoch for ``ts``.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import datetime
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> ts = dfn.lit(
        ...     pa.scalar(datetime(2020, 1, 15, 14, 30, 45),
        ...               type=pa.timestamp('us')))
        >>> r = df.select(dfn.functions.spark.unix_millis(ts).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        1579098645000
    """
    return Expr(_f.unix_millis(col.expr))


def unix_seconds(col: Expr) -> Expr:
    """Spark ``unix_seconds``: seconds since epoch for ``ts``.

    Examples:
        >>> import pyarrow as pa
        >>> from datetime import datetime
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> ts = dfn.lit(
        ...     pa.scalar(datetime(2020, 1, 15, 14, 30, 45),
        ...               type=pa.timestamp('us')))
        >>> r = df.select(dfn.functions.spark.unix_seconds(ts).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        1579098645
    """
    return Expr(_f.unix_seconds(col.expr))


# ---------------------------------------------------------------------------
# Hash functions
# ---------------------------------------------------------------------------


def crc32(col: Expr) -> Expr:
    """Spark ``crc32``: cyclic redundancy check value as a bigint.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"s": ["ABC"]})
        >>> r = df.select(dfn.functions.spark.crc32(dfn.col("s")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        2743272264
    """
    return Expr(_f.crc32(col.expr))


def sha1(col: Expr) -> Expr:
    """Spark ``sha1``: SHA-1 hash as a hex string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"s": ["hello"]})
        >>> r = df.select(dfn.functions.spark.sha1(dfn.col("s")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'
    """
    return Expr(_f.sha1(col.expr))


def sha2(col: Expr, numBits: Expr) -> Expr:  # noqa: N803
    """Spark ``sha2``: SHA-2 family hash (224, 256, 384, 512). Bit length 0 = 256.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"s": ["hello"]})
        >>> r = df.select(
        ...     dfn.functions.spark.sha2(dfn.col("s"), dfn.lit(256)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'
    """
    return Expr(_f.sha2(col.expr, numBits.expr))


def xxhash64(*cols: Expr) -> Expr:
    """Spark ``xxhash64``: 64-bit xxHash of the arguments.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.xxhash64(dfn.lit("hello")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        -4367754540140381902
    """
    return Expr(_f.xxhash64(*[c.expr for c in cols]))


# ---------------------------------------------------------------------------
# JSON functions
# ---------------------------------------------------------------------------


def json_tuple(col: Expr, *fields: Expr) -> Expr:
    """Spark ``json_tuple``: extract top-level fields from a JSON string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.json_tuple(
        ...         dfn.lit('{"a":1,"b":"x"}'), dfn.lit("a"), dfn.lit("b")
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        {'c0': '1', 'c1': 'x'}
    """
    return Expr(_f.json_tuple(col.expr, *[f.expr for f in fields]))


# ---------------------------------------------------------------------------
# Map functions
# ---------------------------------------------------------------------------


def map_from_arrays(col1: Expr, col2: Expr) -> Expr:
    """Spark ``map_from_arrays``: build a map from parallel key/value arrays.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> keys = dfn.functions.spark.array(dfn.lit("a"), dfn.lit("b"))
        >>> vals = dfn.functions.spark.array(dfn.lit(1), dfn.lit(2))
        >>> r = df.select(
        ...     dfn.functions.spark.map_from_arrays(keys, vals).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        [('a', 1), ('b', 2)]
    """
    return Expr(_f.map_from_arrays(col1.expr, col2.expr))


def map_from_entries(col: Expr) -> Expr:
    """Spark ``map_from_entries``: build a map from an array of key/value structs.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.map_from_arrays(
        ...         dfn.functions.spark.array(dfn.lit("a")),
        ...         dfn.functions.spark.array(dfn.lit(1)),
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        [('a', 1)]
    """
    return Expr(_f.map_from_entries(col.expr))


def str_to_map(
    text: Expr,
    pair_delim: Expr | None = None,
    key_value_delim: Expr | None = None,
) -> Expr:
    """Spark ``str_to_map``: split text into key/value pairs using delimiters.

    Delimiters default to ``","`` and ``":"`` when omitted, matching pyspark.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.str_to_map(dfn.lit("a:1,b:2")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        [('a', '1'), ('b', '2')]

        >>> r = df.select(
        ...     dfn.functions.spark.str_to_map(
        ...         dfn.lit("a=1;b=2"),
        ...         pair_delim=dfn.lit(";"),
        ...         key_value_delim=dfn.lit("="),
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        [('a', '1'), ('b', '2')]
    """
    pd = pair_delim if pair_delim is not None else Expr.literal(",")
    kvd = key_value_delim if key_value_delim is not None else Expr.literal(":")
    return Expr(_f.str_to_map(text.expr, pd.expr, kvd.expr))


# ---------------------------------------------------------------------------
# Math functions
# ---------------------------------------------------------------------------


def abs(col: Expr) -> Expr:
    """Spark ``abs``: absolute value.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.abs(dfn.lit(-5)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        5
    """
    return Expr(_f.abs(col.expr))


def ceil(col: Expr) -> Expr:
    """Spark ``ceil``: smallest integer ≥ arg.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.ceil(dfn.lit(1.2)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        2
    """
    return Expr(_f.ceil(col.expr))


def expm1(col: Expr) -> Expr:
    """Spark ``expm1``: exp(arg) - 1.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.expm1(dfn.lit(0.0)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        0.0
    """
    return Expr(_f.expm1(col.expr))


def factorial(col: Expr) -> Expr:
    """Spark ``factorial``: n! for n in [0..20], else NULL.

    Examples:
        >>> import pyarrow as pa
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.factorial(
        ...         dfn.lit(pa.scalar(5, type=pa.int32()))
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        120
    """
    return Expr(_f.factorial(col.expr))


def floor(col: Expr) -> Expr:
    """Spark ``floor``: largest integer ≤ arg.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.floor(dfn.lit(1.8)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        1
    """
    return Expr(_f.floor(col.expr))


def hex(col: Expr) -> Expr:
    """Spark ``hex``: hexadecimal representation.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.hex(dfn.lit(255)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        'FF'
    """
    return Expr(_f.hex(col.expr))


def modulus(dividend: Expr, divisor: Expr) -> Expr:
    """Spark ``mod``: remainder of ``dividend / divisor`` (sign follows dividend).

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.modulus(dfn.lit(10), dfn.lit(3)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        1
    """
    return Expr(_f.modulus(dividend.expr, divisor.expr))


def pmod(dividend: Expr, divisor: Expr) -> Expr:
    """Spark ``pmod``: positive remainder of division.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.pmod(dfn.lit(-1), dfn.lit(3)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        2
    """
    return Expr(_f.pmod(dividend.expr, divisor.expr))


def rint(col: Expr) -> Expr:
    """Spark ``rint``: round to nearest mathematical integer (as double).

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.rint(dfn.lit(2.5)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        2.0
    """
    return Expr(_f.rint(col.expr))


def round(col: Expr, scale: Expr | None = None) -> Expr:
    """Spark ``round``: round to ``scale`` decimal places, HALF_UP rounding.

    ``scale`` defaults to zero when omitted, matching pyspark.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.round(dfn.lit(2.5)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        3.0

        >>> r = df.select(
        ...     dfn.functions.spark.round(
        ...         dfn.lit(2.345), scale=dfn.lit(2)
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        2.35
    """
    scale_expr = scale if scale is not None else _ZERO_I32
    return Expr(_f.round(col.expr, scale_expr.expr))


def unhex(col: Expr) -> Expr:
    r"""Spark ``unhex``: convert hexadecimal string to binary.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.unhex(dfn.lit("FF")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        b'\xff'
    """
    return Expr(_f.unhex(col.expr))


def width_bucket(
    v: Expr,
    min: Expr,
    max: Expr,
    numBucket: Expr,  # noqa: N803
) -> Expr:
    """Spark ``width_bucket``: bucket number for ``value`` in equi-width histogram.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.width_bucket(
        ...         dfn.lit(5.0), dfn.lit(0.0), dfn.lit(10.0), dfn.lit(5)
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        3
    """
    return Expr(_f.width_bucket(v.expr, min.expr, max.expr, numBucket.expr))


def csc(col: Expr) -> Expr:
    """Spark ``csc``: cosecant.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.csc(dfn.lit(1.5708)).alias("v"))
        >>> f"{r.collect_column('v')[0].as_py():.4f}"
        '1.0000'
    """
    return Expr(_f.csc(col.expr))


def sec(col: Expr) -> Expr:
    """Spark ``sec``: secant.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.sec(dfn.lit(0.0)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        1.0
    """
    return Expr(_f.sec(col.expr))


def negative(col: Expr) -> Expr:
    """Spark ``negative``: unary minus.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.negative(dfn.lit(3)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        -3
    """
    return Expr(_f.negative(col.expr))


def bin(col: Expr) -> Expr:
    """Spark ``bin``: binary string representation of a long.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.bin(dfn.lit(7)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        '111'
    """
    return Expr(_f.bin(col.expr))


# ---------------------------------------------------------------------------
# String functions
# ---------------------------------------------------------------------------


def ascii(col: Expr) -> Expr:
    """Spark ``ascii``: code point of the first character.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.ascii(dfn.lit("A")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        65
    """
    return Expr(_f.ascii(col.expr))


def base64(col: Expr) -> Expr:
    """Spark ``base64``: encode binary as a base64 string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.base64(dfn.lit(b"hi")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        'aGk='
    """
    return Expr(_f.base64(col.expr))


def char(col: Expr) -> Expr:
    """Spark ``char``: ASCII character for a code point (mod 256).

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.char(dfn.lit(65)).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        'A'
    """
    return Expr(_f.char(col.expr))


def concat(*cols: Expr) -> Expr:
    """Spark ``concat``: concatenates strings; NULL if any input is NULL.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.concat(dfn.lit("a"), dfn.lit("b")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        'ab'
    """
    return Expr(_f.concat(*[c.expr for c in cols]))


def elt(*inputs: Expr) -> Expr:
    """Spark ``elt``: returns the n-th input (1-indexed).

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.elt(
        ...         dfn.lit(2), dfn.lit("a"), dfn.lit("b")
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        'b'
    """
    return Expr(_f.elt(*[i.expr for i in inputs]))


def ilike(
    str: Expr,
    pattern: Expr,
    escapeChar: str | None = None,  # noqa: N803
) -> Expr:
    """Spark ``ilike``: case-insensitive pattern match.

    ``escapeChar`` is accepted for pyspark parity but is not yet wired through
    the Rust binding; passing a non-``None`` value raises ``NotImplementedError``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.ilike(dfn.lit("HELLO"), dfn.lit("h%")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        True
    """
    if escapeChar is not None:
        msg = "ilike(escapeChar=...) is not yet supported by the Spark UDF binding"
        raise NotImplementedError(msg)
    return Expr(_f.ilike(str.expr, pattern.expr))


def length(col: Expr) -> Expr:
    """Spark ``length``: character length of a string, or byte length of binary.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.length(dfn.lit("hello")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        5
    """
    return Expr(_f.length(col.expr))


def like(
    str: Expr,
    pattern: Expr,
    escapeChar: str | None = None,  # noqa: N803
) -> Expr:
    """Spark ``like``: case-sensitive pattern match.

    ``escapeChar`` is accepted for pyspark parity but is not yet wired through
    the Rust binding; passing a non-``None`` value raises ``NotImplementedError``.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.like(dfn.lit("hello"), dfn.lit("h%")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        True
    """
    if escapeChar is not None:
        msg = "like(escapeChar=...) is not yet supported by the Spark UDF binding"
        raise NotImplementedError(msg)
    return Expr(_f.like(str.expr, pattern.expr))


def luhn_check(col: Expr) -> Expr:
    """Spark ``luhn_check``: true if the digit string passes the Luhn check.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.luhn_check(
        ...         dfn.lit("4111111111111111")
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        True
    """
    return Expr(_f.luhn_check(col.expr))


def format_string(format: str | Expr, *cols: Expr) -> Expr:
    """Spark ``format_string``: printf-style format string.

    ``format`` is the printf-style template (a plain ``str`` is auto-promoted
    to a literal expression); remaining args are values to substitute.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.format_string(
        ...         "%d-%s", dfn.lit(42), dfn.lit("hi")
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        '42-hi'
    """
    fmt_expr = format if isinstance(format, Expr) else Expr.literal(format)
    return Expr(_f.format_string(fmt_expr.expr, *[c.expr for c in cols]))


def space(col: Expr) -> Expr:
    """Spark ``space``: string of n spaces.

    Examples:
        >>> import pyarrow as pa
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.space(
        ...         dfn.lit(pa.scalar(3, type=pa.int32()))
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        '   '
    """
    return Expr(_f.space(col.expr))


def substring(str: Expr, pos: Expr, len: Expr) -> Expr:
    """Spark ``substring``: 1-indexed substring starting at ``pos`` of given ``length``.

    Negative ``pos`` counts from the end.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.substring(
        ...         dfn.lit("hello"), dfn.lit(1), dfn.lit(3)
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        'hel'
    """
    return Expr(_f.substring(str.expr, pos.expr, len.expr))


def unbase64(col: Expr) -> Expr:
    """Spark ``unbase64``: decode a base64 string to binary.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.unbase64(dfn.lit("aGk=")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        b'hi'
    """
    return Expr(_f.unbase64(col.expr))


def soundex(col: Expr) -> Expr:
    """Spark ``soundex``: Soundex phonetic code.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(dfn.functions.spark.soundex(dfn.lit("Robert")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        'R163'
    """
    return Expr(_f.soundex(col.expr))


def is_valid_utf8(str: Expr) -> Expr:
    """Spark ``is_valid_utf8``: true if the string is valid UTF-8.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.is_valid_utf8(dfn.lit("hello")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        True
    """
    return Expr(_f.is_valid_utf8(str.expr))


def make_valid_utf8(str: Expr) -> Expr:
    """Spark ``make_valid_utf8``: replace invalid UTF-8 bytes with U+FFFD.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.make_valid_utf8(dfn.lit("hello")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        'hello'
    """
    return Expr(_f.make_valid_utf8(str.expr))


# ---------------------------------------------------------------------------
# URL functions
# ---------------------------------------------------------------------------


def parse_url(
    url: Expr,
    partToExtract: Expr,  # noqa: N803
    key: Expr | None = None,
) -> Expr:
    """Spark ``parse_url``: extract a part from a URL; errors on invalid URLs.

    ``partToExtract`` is one of ``"HOST"``, ``"PATH"``, ``"QUERY"``,
    ``"REF"``, ``"PROTOCOL"``, ``"FILE"``, ``"AUTHORITY"``, ``"USERINFO"``.
    Pass ``key`` only with ``"QUERY"`` to extract a single parameter.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.parse_url(
        ...         dfn.lit("http://example.com/path?q=1"), dfn.lit("HOST")
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        'example.com'

        >>> r = df.select(
        ...     dfn.functions.spark.parse_url(
        ...         dfn.lit("http://example.com/path?q=1"),
        ...         dfn.lit("QUERY"),
        ...         key=dfn.lit("q"),
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        '1'
    """
    if key is None:
        return Expr(_f.parse_url(url.expr, partToExtract.expr))
    return Expr(_f.parse_url(url.expr, partToExtract.expr, key.expr))


def try_parse_url(
    url: Expr,
    partToExtract: Expr,  # noqa: N803
    key: Expr | None = None,
) -> Expr:
    """Spark ``try_parse_url``: like ``parse_url`` but returns NULL on invalid URLs.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.try_parse_url(
        ...         dfn.lit("http://example.com/"), dfn.lit("HOST")
        ...     ).alias("v")
        ... )
        >>> r.collect_column("v")[0].as_py()
        'example.com'
    """
    if key is None:
        return Expr(_f.try_parse_url(url.expr, partToExtract.expr))
    return Expr(_f.try_parse_url(url.expr, partToExtract.expr, key.expr))


def url_decode(str: Expr) -> Expr:
    """Spark ``url_decode``: decode an application/x-www-form-urlencoded string.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.url_decode(dfn.lit("a%20b")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        'a b'
    """
    return Expr(_f.url_decode(str.expr))


def try_url_decode(str: Expr) -> Expr:
    """Spark ``try_url_decode``: like ``url_decode``; returns NULL on invalid input.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.try_url_decode(dfn.lit("a%20b")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        'a b'
    """
    return Expr(_f.try_url_decode(str.expr))


def url_encode(str: Expr) -> Expr:
    """Spark ``url_encode``: encode a string in application/x-www-form-urlencoded.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"x": [1]})
        >>> r = df.select(
        ...     dfn.functions.spark.url_encode(dfn.lit("a b")).alias("v"))
        >>> r.collect_column("v")[0].as_py()
        'a+b'
    """
    return Expr(_f.url_encode(str.expr))


__all__ = [
    # Math
    "abs",
    # Datetime
    "add_months",
    "array",
    # Array
    "array_contains",
    "array_repeat",
    # String
    "ascii",
    # Aggregate
    "avg",
    "base64",
    "bin",
    "bit_count",
    # Bitwise
    "bit_get",
    "bitmap_bit_position",
    "bitmap_bucket_number",
    # Bitmap
    "bitmap_count",
    "bitwise_not",
    "ceil",
    "char",
    "collect_list",
    "collect_set",
    "concat",
    # Hash
    "crc32",
    "csc",
    "date_add",
    "date_diff",
    "date_part",
    "date_sub",
    "date_trunc",
    "elt",
    "expm1",
    "factorial",
    "floor",
    "format_string",
    "from_utc_timestamp",
    "hex",
    "hour",
    "if_",
    "ilike",
    "is_valid_utf8",
    # JSON
    "json_tuple",
    "last_day",
    "length",
    "like",
    "luhn_check",
    "make_dt_interval",
    "make_interval",
    "make_valid_utf8",
    # Map
    "map_from_arrays",
    "map_from_entries",
    "minute",
    "modulus",
    "negative",
    "next_day",
    # URL
    "parse_url",
    "pmod",
    "rint",
    "round",
    "sec",
    "second",
    "sha1",
    "sha2",
    "shiftleft",
    "shiftright",
    "shiftrightunsigned",
    "shuffle",
    # Collection / Conditional / Conversion
    "size",
    "slice",
    "soundex",
    "space",
    "spark_cast",
    "str_to_map",
    "substring",
    "time_trunc",
    "to_utc_timestamp",
    "trunc",
    "try_parse_url",
    "try_sum",
    "try_url_decode",
    "unbase64",
    "unhex",
    "unix_date",
    "unix_micros",
    "unix_millis",
    "unix_seconds",
    "url_decode",
    "url_encode",
    "width_bucket",
    "xxhash64",
]
