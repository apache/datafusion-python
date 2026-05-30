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

"""Tests for the Spark-compatible function bindings."""

import pyarrow as pa
import pytest
from datafusion import SessionContext, col, lit
from datafusion import functions as f
from datafusion.functions import spark


@pytest.fixture
def df():
    ctx = SessionContext()
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(["hello", "WORLD", "abc"]),
            pa.array([1, 2, 3]),
            pa.array([-5, 0, 7]),
            pa.array([1.0, 2.5, 3.0]),
            pa.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
        ],
        names=["s", "i", "n", "f", "a"],
    )
    return ctx.create_dataframe([[batch]])


def _val(df, expr):
    return df.select(expr.alias("v")).collect_column("v")[0].as_py()


# ---------------------------------------------------------------------------
# Math
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("expr_factory", "expected"),
    [
        (lambda: spark.abs(lit(-5)), 5),
        (lambda: spark.ceil(lit(1.2)), 2),
        (lambda: spark.floor(lit(1.8)), 1),
        (lambda: spark.bin(lit(7)), "111"),
        (lambda: spark.hex(lit(255)), "FF"),
        (lambda: spark.modulus(lit(10), lit(3)), 1),
        (lambda: spark.pmod(lit(-1), lit(3)), 2),
        (lambda: spark.rint(lit(2.5)), 2.0),
        (lambda: spark.round(lit(2.5), lit(0)), 3.0),
        (lambda: spark.negative(lit(3)), -3),
    ],
)
def test_math(df, expr_factory, expected):
    assert _val(df, expr_factory()) == expected


def test_factorial(df):
    # factorial wants Int32; lit(int) is Int64 by default.
    expr = spark.factorial(lit(pa.scalar(5, type=pa.int32())))
    assert _val(df, expr) == 120


# ---------------------------------------------------------------------------
# String
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("expr_factory", "expected"),
    [
        (lambda: spark.ascii(lit("A")), 65),
        (lambda: spark.char(lit(65)), "A"),
        (lambda: spark.length(lit("hello")), 5),
        (lambda: spark.like(lit("hello"), lit("h%")), True),
        (lambda: spark.ilike(lit("HELLO"), lit("h%")), True),
        (lambda: spark.substring(lit("hello"), lit(1), lit(3)), "hel"),
        (lambda: spark.soundex(lit("Robert")), "R163"),
        (lambda: spark.is_valid_utf8(lit("hi")), True),
        (lambda: spark.concat(lit("a"), lit("b")), "ab"),
        (lambda: spark.elt(lit(2), lit("a"), lit("b")), "b"),
    ],
)
def test_string(df, expr_factory, expected):
    assert _val(df, expr_factory()) == expected


def test_space(df):
    expr = spark.space(lit(pa.scalar(3, type=pa.int32())))
    assert _val(df, expr) == "   "


# ---------------------------------------------------------------------------
# Hash
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("expr_factory", "expected"),
    [
        (
            lambda: spark.sha1(lit("hello")),
            "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
        ),
        (
            lambda: spark.sha2(lit("hello"), lit(256)),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
        ),
        (lambda: spark.crc32(lit("ABC")), 2743272264),
    ],
)
def test_hash(df, expr_factory, expected):
    assert _val(df, expr_factory()) == expected


# ---------------------------------------------------------------------------
# Bitwise
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("expr_factory", "expected"),
    [
        (lambda: spark.bit_count(lit(7)), 3),
        (lambda: spark.bit_get(lit(5), lit(0)), 1),
        (lambda: spark.bitwise_not(lit(0)), -1),
        (lambda: spark.shiftleft(lit(1), lit(3)), 8),
        (lambda: spark.shiftright(lit(8), lit(2)), 2),
        (lambda: spark.shiftrightunsigned(lit(8), lit(2)), 2),
    ],
)
def test_bitwise(df, expr_factory, expected):
    assert _val(df, expr_factory()) == expected


# ---------------------------------------------------------------------------
# Array / collection / conditional
# ---------------------------------------------------------------------------


def test_array_and_size(df):
    arr = spark.array(lit(1), lit(2), lit(3))
    assert _val(df, arr) == [1, 2, 3]
    assert _val(df, spark.size(arr)) == 3
    assert _val(df, spark.array_contains(arr, lit(2))) is True


def test_slice(df):
    arr = spark.array(lit(1), lit(2), lit(3), lit(4))
    assert _val(df, spark.slice(arr, lit(2), lit(2))) == [2, 3]


def test_array_repeat(df):
    assert _val(df, spark.array_repeat(lit("a"), lit(3))) == ["a", "a", "a"]


def test_if(df):
    assert _val(df, spark.if_(lit(True), lit("yes"), lit("no"))) == "yes"
    assert _val(df, spark.if_(lit(False), lit("yes"), lit("no"))) == "no"


# ---------------------------------------------------------------------------
# Aggregate
# ---------------------------------------------------------------------------


def test_aggregate(df):
    r = df.aggregate(
        [],
        [
            spark.avg(col("f")).alias("avg"),
            spark.try_sum(col("i")).alias("sum"),
        ],
    ).collect()
    rec = pa.Table.from_batches(r)
    assert rec.column("avg")[0].as_py() == pytest.approx(2.1666666, rel=1e-3)
    assert rec.column("sum")[0].as_py() == 6


def test_collect_list_set(df):
    r = df.aggregate(
        [],
        [
            spark.collect_list(col("i")).alias("cl"),
            spark.collect_set(col("i")).alias("cs"),
        ],
    ).collect()
    rec = pa.Table.from_batches(r)
    assert sorted(rec.column("cl")[0].as_py()) == [1, 2, 3]
    assert sorted(rec.column("cs")[0].as_py()) == [1, 2, 3]


# ---------------------------------------------------------------------------
# Spark-semantics conflicts vs DataFusion defaults
# ---------------------------------------------------------------------------


def test_concat_null_propagates():
    """Spark concat returns NULL on any NULL input; default skips NULLs."""
    ctx = SessionContext()
    df = ctx.from_pydict({"x": [1]})
    default_out = (
        df.select(f.concat(lit("a"), lit(None), lit("b")).alias("v"))
        .collect_column("v")[0]
        .as_py()
    )
    spark_out = _val(df, spark.concat(lit("a"), lit(None), lit("b")))
    assert default_out == "ab"
    assert spark_out is None


def test_round_half_up():
    """Spark round uses HALF_UP rounding (2.5 → 3)."""
    ctx = SessionContext()
    df = ctx.from_pydict({"x": [1]})
    assert _val(df, spark.round(lit(2.5), lit(0))) == 3.0


# ---------------------------------------------------------------------------
# Optional parameter defaults / NotImplementedError
# ---------------------------------------------------------------------------


def test_round_scale_default():
    """spark.round defaults scale to 0."""
    ctx = SessionContext()
    df = ctx.from_pydict({"x": [1]})
    assert _val(df, spark.round(lit(2.5))) == 3.0


def test_make_dt_interval_defaults():
    """spark.make_dt_interval with no args returns a zero day-time interval."""
    import datetime as dt

    ctx = SessionContext()
    df = ctx.from_pydict({"x": [1]})
    assert _val(df, spark.make_dt_interval()) == dt.timedelta(0)


def test_make_interval_defaults():
    """spark.make_interval with no args returns a zero interval."""
    ctx = SessionContext()
    df = ctx.from_pydict({"x": [1]})
    assert _val(df, spark.make_interval()).months == 0


def test_str_to_map_defaults():
    """spark.str_to_map defaults delimiters to ',' and ':'."""
    ctx = SessionContext()
    df = ctx.from_pydict({"x": [1]})
    assert _val(df, spark.str_to_map(lit("a:1,b:2"))) == [("a", "1"), ("b", "2")]


def test_shuffle_seed_raises():
    """spark.shuffle(seed=...) raises NotImplementedError until Rust supports it."""
    with pytest.raises(NotImplementedError, match="seed"):
        spark.shuffle(spark.array(lit(1), lit(2)), seed=1)


def test_like_escape_raises():
    """spark.like/ilike escapeChar raises NotImplementedError until Rust supports."""
    with pytest.raises(NotImplementedError, match="escapeChar"):
        spark.like(lit("a"), lit("a"), escapeChar="\\")
    with pytest.raises(NotImplementedError, match="escapeChar"):
        spark.ilike(lit("a"), lit("a"), escapeChar="\\")


def test_parse_url_three_arg():
    """parse_url(url, partToExtract, key=...) extracts query parameters."""
    ctx = SessionContext()
    df = ctx.from_pydict({"x": [1]})
    url = lit("http://example.com/p?q=hello&n=1")
    assert _val(df, spark.parse_url(url, lit("QUERY"), key=lit("q"))) == "hello"
    assert _val(df, spark.try_parse_url(url, lit("QUERY"), key=lit("n"))) == "1"


def test_format_string_plain_str_format():
    """format_string accepts a plain str format that is auto-promoted to lit."""
    ctx = SessionContext()
    df = ctx.from_pydict({"x": [1]})
    assert _val(df, spark.format_string("%d-%s", lit(42), lit("hi"))) == "42-hi"


def test_aggregate_positional_compat():
    """Pyspark-style positional calls still work after the rename to ``col``."""
    ctx = SessionContext()
    df = ctx.from_pydict({"a": [1.0, 2.0, 3.0]})
    out = df.aggregate(
        [],
        [
            spark.avg(col("a")).alias("av"),
            spark.try_sum(col("a")).alias("ts"),
            spark.collect_list(col("a")).alias("cl"),
            spark.collect_set(col("a")).alias("cs"),
        ],
    ).collect()
    rec = pa.Table.from_batches(out)
    assert rec.column("av")[0].as_py() == 2.0
    assert rec.column("ts")[0].as_py() == 6.0


# ---------------------------------------------------------------------------
# SQL path via enable_spark_functions
# ---------------------------------------------------------------------------


def test_sql_requires_enable():
    """Spark-only function (xxhash64) is not in SQL namespace by default."""
    ctx = SessionContext()
    with pytest.raises(Exception, match=r"(?i)xxhash64|Invalid function"):
        ctx.sql("SELECT xxhash64('hello')").collect()


def test_sql_after_enable():
    ctx = SessionContext()
    ctx.enable_spark_functions()
    out = ctx.sql("SELECT sha2('hello', 256) AS h").collect_column("h")[0].as_py()
    assert out == ("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")


def test_sql_concat_semantics_override():
    """After enable, SQL concat propagates NULL like Spark."""
    ctx = SessionContext()
    default_out = (
        ctx.sql("SELECT concat('a', NULL, 'b') AS c").collect_column("c")[0].as_py()
    )
    assert default_out == "ab"

    ctx2 = SessionContext()
    ctx2.enable_spark_functions()
    spark_out = (
        ctx2.sql("SELECT concat('a', NULL, 'b') AS c").collect_column("c")[0].as_py()
    )
    assert spark_out is None
