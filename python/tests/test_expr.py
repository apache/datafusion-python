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

from datetime import datetime, timezone

import pyarrow as pa
import pytest
from datafusion import (
    SessionContext,
    col,
    functions,
    lit,
    lit_with_metadata,
    literal_with_metadata,
)
from datafusion.expr import (
    Aggregate,
    AggregateFunction,
    BinaryExpr,
    Column,
    CopyTo,
    CreateIndex,
    DescribeTable,
    DmlStatement,
    DropCatalogSchema,
    Filter,
    Limit,
    Literal,
    Projection,
    RecursiveQuery,
    Sort,
    TableScan,
    TransactionEnd,
    TransactionStart,
    Values,
)


@pytest.fixture
def test_ctx():
    ctx = SessionContext()
    ctx.register_csv("test", "testing/data/csv/aggregate_test_100.csv")
    return ctx


def test_projection(test_ctx):
    df = test_ctx.sql("select c1, 123, c1 < 123 from test")
    plan = df.logical_plan()

    plan = plan.to_variant()
    assert isinstance(plan, Projection)

    expr = plan.projections()

    col1 = expr[0].to_variant()
    assert isinstance(col1, Column)
    assert col1.name() == "c1"
    assert col1.qualified_name() == "test.c1"

    col2 = expr[1].to_variant()
    assert isinstance(col2, Literal)
    assert col2.data_type() == "Int64"
    assert col2.value_i64() == 123

    col3 = expr[2].to_variant()
    assert isinstance(col3, BinaryExpr)
    assert isinstance(col3.left().to_variant(), Column)
    assert col3.op() == "<"
    assert isinstance(col3.right().to_variant(), Literal)

    plan = plan.input()[0].to_variant()
    assert isinstance(plan, TableScan)


def test_filter(test_ctx):
    df = test_ctx.sql("select c1 from test WHERE c1 > 5")
    plan = df.logical_plan()

    plan = plan.to_variant()
    assert isinstance(plan, Projection)

    plan = plan.input()[0].to_variant()
    assert isinstance(plan, Filter)


def test_limit(test_ctx):
    df = test_ctx.sql("select c1 from test LIMIT 10")
    plan = df.logical_plan()

    plan = plan.to_variant()
    assert isinstance(plan, Limit)
    assert "Skip: None" in str(plan)

    df = test_ctx.sql("select c1 from test LIMIT 10 OFFSET 5")
    plan = df.logical_plan()

    plan = plan.to_variant()
    assert isinstance(plan, Limit)
    assert "Skip: Some(Literal(Int64(5), None))" in str(plan)


def test_aggregate_query(test_ctx):
    df = test_ctx.sql("select c1, count(*) from test group by c1")
    plan = df.logical_plan()

    projection = plan.to_variant()
    assert isinstance(projection, Projection)

    aggregate = projection.input()[0].to_variant()
    assert isinstance(aggregate, Aggregate)

    col1 = aggregate.group_by_exprs()[0].to_variant()
    assert isinstance(col1, Column)
    assert col1.name() == "c1"
    assert col1.qualified_name() == "test.c1"

    col2 = aggregate.aggregate_exprs()[0].to_variant()
    assert isinstance(col2, AggregateFunction)


def test_sort(test_ctx):
    df = test_ctx.sql("select c1 from test order by c1")
    plan = df.logical_plan()

    plan = plan.to_variant()
    assert isinstance(plan, Sort)


def test_relational_expr(test_ctx):
    ctx = SessionContext()

    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1, 2, 3]),
            pa.array(["alpha", "beta", "gamma"], type=pa.string_view()),
        ],
        names=["a", "b"],
    )
    df = ctx.create_dataframe([[batch]], name="batch_array")

    assert df.filter(col("a") == 1).count() == 1
    assert df.filter(col("a") != 1).count() == 2
    assert df.filter(col("a") >= 1).count() == 3
    assert df.filter(col("a") > 1).count() == 2
    assert df.filter(col("a") <= 3).count() == 3
    assert df.filter(col("a") < 3).count() == 2

    assert df.filter(col("b") == "beta").count() == 1
    assert df.filter(col("b") != "beta").count() == 2

    assert df.filter(col("a") == "beta").count() == 0


def test_expr_to_variant():
    # Taken from https://github.com/apache/datafusion-python/issues/781
    from datafusion import SessionContext
    from datafusion.expr import Filter

    def traverse_logical_plan(plan):
        cur_node = plan.to_variant()
        if isinstance(cur_node, Filter):
            return cur_node.predicate().to_variant()
        if hasattr(plan, "inputs"):
            for input_plan in plan.inputs():
                res = traverse_logical_plan(input_plan)
                if res is not None:
                    return res
        return None

    ctx = SessionContext()
    data = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}
    ctx.from_pydict(data, name="table1")
    query = "SELECT * FROM table1 t1 WHERE t1.name IN ('dfa', 'ad', 'dfre', 'vsa')"
    logical_plan = ctx.sql(query).optimized_logical_plan()
    variant = traverse_logical_plan(logical_plan)
    assert variant is not None
    assert variant.expr().to_variant().qualified_name() == "table1.name"
    assert (
        str(variant.list())
        == '[Expr(Utf8("dfa")), Expr(Utf8("ad")), Expr(Utf8("dfre")), Expr(Utf8("vsa"))]'  # noqa: E501
    )
    assert not variant.negated()


def test_expr_getitem() -> None:
    ctx = SessionContext()
    data = {
        "array_values": [[1, 2, 3], [4, 5], [6], []],
        "struct_values": [
            {"name": "Alice", "age": 15},
            {"name": "Bob", "age": 14},
            {"name": "Charlie", "age": 13},
            {"name": None, "age": 12},
        ],
    }
    df = ctx.from_pydict(data, name="table1")

    names = df.select(col("struct_values")["name"].alias("name")).collect()
    names = [r.as_py() for rs in names for r in rs["name"]]

    array_values = df.select(col("array_values")[1].alias("value")).collect()
    array_values = [r.as_py() for rs in array_values for r in rs["value"]]

    assert names == ["Alice", "Bob", "Charlie", None]
    assert array_values == [2, 5, None, None]


def test_display_name_deprecation():
    import warnings

    expr = col("foo")
    with warnings.catch_warnings(record=True) as w:
        # Cause all warnings to always be triggered
        warnings.simplefilter("always")

        # should trigger warning
        name = expr.display_name()

        # Verify some things
        assert len(w) == 1
        assert issubclass(w[-1].category, DeprecationWarning)
        assert "deprecated" in str(w[-1].message)

    # returns appropriate result
    assert name == expr.schema_name()
    assert name == "foo"


@pytest.fixture
def df():
    ctx = SessionContext()

    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, None]), pa.array([4, None, 6]), pa.array([None, None, 8])],
        names=["a", "b", "c"],
    )

    return ctx.from_arrow(batch)


def test_fill_null(df):
    df = df.select(
        col("a").fill_null(100).alias("a"),
        col("b").fill_null(25).alias("b"),
        col("c").fill_null(1234).alias("c"),
    )
    df.show()
    result = df.collect()[0]

    assert result.column(0) == pa.array([1, 2, 100])
    assert result.column(1) == pa.array([4, 25, 6])
    assert result.column(2) == pa.array([1234, 1234, 8])


def test_copy_to():
    ctx = SessionContext()
    ctx.sql("CREATE TABLE foo (a int, b int)").collect()
    df = ctx.sql("COPY foo TO bar STORED AS CSV")
    plan = df.logical_plan()
    plan = plan.to_variant()
    assert isinstance(plan, CopyTo)


def test_create_index():
    ctx = SessionContext()
    ctx.sql("CREATE TABLE foo (a int, b int)").collect()
    plan = ctx.sql("create index idx on foo (a)").logical_plan()
    plan = plan.to_variant()
    assert isinstance(plan, CreateIndex)


def test_describe_table():
    ctx = SessionContext()
    ctx.sql("CREATE TABLE foo (a int, b int)").collect()
    plan = ctx.sql("describe foo").logical_plan()
    plan = plan.to_variant()
    assert isinstance(plan, DescribeTable)


def test_dml_statement():
    ctx = SessionContext()
    ctx.sql("CREATE TABLE foo (a int, b int)").collect()
    plan = ctx.sql("insert into foo values (1, 2)").logical_plan()
    plan = plan.to_variant()
    assert isinstance(plan, DmlStatement)


def drop_catalog_schema():
    ctx = SessionContext()
    plan = ctx.sql("drop schema cat").logical_plan()
    plan = plan.to_variant()
    assert isinstance(plan, DropCatalogSchema)


def test_recursive_query():
    ctx = SessionContext()
    plan = ctx.sql(
        """
        WITH RECURSIVE cte AS (
        SELECT 1 as n
        UNION ALL
        SELECT n + 1 FROM cte WHERE n < 5
        )
        SELECT * FROM cte;
        """
    ).logical_plan()
    plan = plan.inputs()[0].inputs()[0].to_variant()
    assert isinstance(plan, RecursiveQuery)


def test_values():
    ctx = SessionContext()
    plan = ctx.sql("values (1, 'foo'), (2, 'bar')").logical_plan()
    plan = plan.to_variant()
    assert isinstance(plan, Values)


def test_transaction_start():
    ctx = SessionContext()
    plan = ctx.sql("START TRANSACTION").logical_plan()
    plan = plan.to_variant()
    assert isinstance(plan, TransactionStart)


def test_transaction_end():
    ctx = SessionContext()
    plan = ctx.sql("COMMIT").logical_plan()
    plan = plan.to_variant()
    assert isinstance(plan, TransactionEnd)


def test_col_getattr():
    ctx = SessionContext()
    data = {
        "array_values": [[1, 2, 3], [4, 5], [6], []],
        "struct_values": [
            {"name": "Alice", "age": 15},
            {"name": "Bob", "age": 14},
            {"name": "Charlie", "age": 13},
            {"name": None, "age": 12},
        ],
    }
    df = ctx.from_pydict(data, name="table1")

    names = df.select(col.struct_values["name"].alias("name")).collect()
    names = [r.as_py() for rs in names for r in rs["name"]]

    array_values = df.select(col.array_values[1].alias("value")).collect()
    array_values = [r.as_py() for rs in array_values for r in rs["value"]]

    assert names == ["Alice", "Bob", "Charlie", None]
    assert array_values == [2, 5, None, None]


def test_alias_with_metadata(df):
    df = df.select(col("a").alias("b", {"key": "value"}))
    assert df.schema().field("b").metadata == {b"key": b"value"}


# These unit tests are to ensure the expression functions do not regress
# For the math functions we will use `functions.round` so we can more
# easily test for equivalence and not worry about floating point precision
@pytest.mark.parametrize(
    ("function", "expected_result"),
    [
        # Math Functions
        pytest.param(
            functions.round(col("a").asin(), lit(4)),
            pa.array([-0.8481, 0.5236, 0.0, None], type=pa.float64()),
            id="asin",
        ),
        pytest.param(
            functions.round(col("a").sin(), lit(4)),
            pa.array([-0.6816, 0.4794, 0.0, None], type=pa.float64()),
            id="sin",
        ),
        pytest.param(
            # Since log10 of negative returns NaN and you can't test NaN for
            # equivalence, also do an abs() here.
            functions.round(col("a").abs().log10(), lit(4)),
            pa.array([-0.1249, -0.301, -float("inf"), None], type=pa.float64()),
            id="log10",
        ),
        pytest.param(
            col("a").iszero(),
            pa.array([False, False, True, None], type=pa.bool_()),
            id="iszero",
        ),
        pytest.param(
            functions.round(col("a").acos(), lit(4)),
            pa.array([2.4189, 1.0472, 1.5708, None], type=pa.float64()),
            id="acos",
        ),
        pytest.param(
            col("e").isnan(),
            pa.array([False, True, False, None], type=pa.bool_()),
            id="isnan",
        ),
        pytest.param(
            functions.round(col("a").degrees(), lit(4)),
            pa.array([-42.9718, 28.6479, 0.0, None], type=pa.float64()),
            id="degrees",
        ),
        pytest.param(
            functions.round(col("a").asinh(), lit(4)),
            pa.array([-0.6931, 0.4812, 0.0, None], type=pa.float64()),
            id="asinh",
        ),
        pytest.param(
            col("a").abs(),
            pa.array([0.75, 0.5, 0.0, None], type=pa.float64()),
            id="abs",
        ),
        pytest.param(
            functions.round(col("a").exp(), lit(4)),
            pa.array([0.4724, 1.6487, 1.0, None], type=pa.float64()),
            id="exp",
        ),
        pytest.param(
            functions.round(col("a").cosh(), lit(4)),
            pa.array([1.2947, 1.1276, 1.0, None], type=pa.float64()),
            id="cosh",
        ),
        pytest.param(
            functions.round(col("a").radians(), lit(4)),
            pa.array([-0.0131, 0.0087, 0.0, None], type=pa.float64()),
            id="radians",
        ),
        pytest.param(
            functions.round(col("a").abs().sqrt(), lit(4)),
            pa.array([0.866, 0.7071, 0.0, None], type=pa.float64()),
            id="sqrt",
        ),
        pytest.param(
            functions.round(col("a").tanh(), lit(4)),
            pa.array([-0.6351, 0.4621, 0.0, None], type=pa.float64()),
            id="tanh",
        ),
        pytest.param(
            functions.round(col("a").atan(), lit(4)),
            pa.array([-0.6435, 0.4636, 0.0, None], type=pa.float64()),
            id="atan",
        ),
        pytest.param(
            functions.round(col("a").atanh(), lit(4)),
            pa.array([-0.973, 0.5493, 0.0, None], type=pa.float64()),
            id="atanh",
        ),
        pytest.param(
            # large numbers cause an integer overflow so divid to make smaller
            (col("b") / lit(4)).factorial(),
            pa.array([1, 3628800, 1, None], type=pa.int64()),
            id="factorial",
        ),
        pytest.param(
            # Valid values of acosh must be >= 1.0
            functions.round((col("a").abs() + lit(1.0)).acosh(), lit(4)),
            pa.array([1.1588, 0.9624, 0.0, None], type=pa.float64()),
            id="acosh",
        ),
        pytest.param(
            col("a").floor(),
            pa.array([-1.0, 0.0, 0.0, None], type=pa.float64()),
            id="floor",
        ),
        pytest.param(
            col("a").ceil(),
            pa.array([-0.0, 1.0, 0.0, None], type=pa.float64()),
            id="ceil",
        ),
        pytest.param(
            functions.round(col("a").abs().ln(), lit(4)),
            pa.array([-0.2877, -0.6931, float("-inf"), None], type=pa.float64()),
            id="ln",
        ),
        pytest.param(
            functions.round(col("a").tan(), lit(4)),
            pa.array([-0.9316, 0.5463, 0.0, None], type=pa.float64()),
            id="tan",
        ),
        pytest.param(
            functions.round(col("a").cbrt(), lit(4)),
            pa.array([-0.9086, 0.7937, 0.0, None], type=pa.float64()),
            id="cbrt",
        ),
        pytest.param(
            functions.round(col("a").cos(), lit(4)),
            pa.array([0.7317, 0.8776, 1.0, None], type=pa.float64()),
            id="cos",
        ),
        pytest.param(
            functions.round(col("a").sinh(), lit(4)),
            pa.array([-0.8223, 0.5211, 0.0, None], type=pa.float64()),
            id="sinh",
        ),
        pytest.param(
            col("a").signum(),
            pa.array([-1.0, 1.0, 0.0, None], type=pa.float64()),
            id="signum",
        ),
        pytest.param(
            functions.round(col("a").abs().log2(), lit(4)),
            pa.array([-0.415, -1.0, float("-inf"), None], type=pa.float64()),
            id="log2",
        ),
        pytest.param(
            functions.round(col("a").cot(), lit(4)),
            pa.array([-1.0734, 1.8305, float("inf"), None], type=pa.float64()),
            id="cot",
        ),
        #
        # String Functions
        #
        pytest.param(
            col("c").reverse(),
            pa.array(["olleH", " dlrow ", "!", None], type=pa.string()),
            id="reverse",
        ),
        pytest.param(
            col("c").bit_length(),
            pa.array([40, 56, 8, None], type=pa.int32()),
            id="bit_length",
        ),
        pytest.param(
            col("b").to_hex(),
            pa.array(["ffffffffffffffe2", "2a", "0", None], type=pa.string()),
            id="to_hex",
        ),
        pytest.param(
            col("c").length(),
            pa.array([5, 7, 1, None], type=pa.int32()),
            id="length",
        ),
        pytest.param(
            col("c").lower(),
            pa.array(["hello", " world ", "!", None], type=pa.string()),
            id="lower",
        ),
        pytest.param(
            col("c").ascii(),
            pa.array([72, 32, 33, None], type=pa.int32()),
            id="ascii",
        ),
        pytest.param(
            col("c").sha512(),
            pa.array(
                [
                    bytes.fromhex(
                        "3615F80C9D293ED7402687F94B22D58E529B8CC7916F8FAC7FDDF7FBD5AF4CF777D3D795A7A00A16BF7E7F3FB9561EE9BAAE480DA9FE7A18769E71886B03F315"
                    ),
                    bytes.fromhex(
                        "A6758FDA3C2F0B554084E18308EA99B94B54EEE8FDA72697CEA7844E524CC2F2F2EE4CC8BAC87D2E3E7222959FE3D0CA1A841761FDC0D1780F6FE9E39E369500"
                    ),
                    bytes.fromhex(
                        "3831A6A6155E509DEE59A7F451EB35324D8F8F2DF6E3708894740F98FDEE23889F4DE5ADB0C5010DFB555CDA77C8AB5DC902094C52DE3278F35A75EBC25F093A"
                    ),
                    None,
                ],
                type=pa.binary(),
            ),
            id="sha512",
        ),
        pytest.param(
            col("c").sha384(),
            pa.array(
                [
                    bytes.fromhex(
                        "3519FE5AD2C596EFE3E276A6F351B8FC0B03DB861782490D45F7598EBD0AB5FD5520ED102F38C4A5EC834E98668035FC"
                    ),
                    bytes.fromhex(
                        "A6A38A9AE2CFD0D67F49989AD584632BF7D7A07DAD2277E92326A6A0B37F884A871D6173FB342CFE258E375258ACAAEC"
                    ),
                    bytes.fromhex(
                        "1D0EC8C84EE9521E21F06774DE232367B64DE628474CB5B2E372B699A1F55AE335CC37193EF823E33324DFD9A70738A6"
                    ),
                    None,
                ],
                type=pa.binary(),
            ),
            id="sha384",
        ),
        pytest.param(
            col("c").sha256(),
            pa.array(
                [
                    bytes.fromhex(
                        "185F8DB32271FE25F561A6FC938B2E264306EC304EDA518007D1764826381969"
                    ),
                    bytes.fromhex(
                        "DE2EF0D77D456EC1CDE2C52F75996F6636A64079297213D548D875A488B03A75"
                    ),
                    bytes.fromhex(
                        "BB7208BC9B5D7C04F1236A82A0093A5E33F40423D5BA8D4266F7092C3BA43B62"
                    ),
                    None,
                ],
                type=pa.binary(),
            ),
            id="sha256",
        ),
        pytest.param(
            col("c").sha224(),
            pa.array(
                [
                    bytes.fromhex(
                        "4149DA18AA8BFC2B1E382C6C26556D01A92C261B6436DAD5E3BE3FCC"
                    ),
                    bytes.fromhex(
                        "AD6DF6D9ECDDF50AF2A72D5E3144BA813EE954537572C0E8AB3066BE"
                    ),
                    bytes.fromhex(
                        "6641A7E8278BCD49E476E7ACAE158F4105B2952D22AEB2E0B9A231A0"
                    ),
                    None,
                ],
                type=pa.binary(),
            ),
            id="sha224",
        ),
        pytest.param(
            col("c").btrim(),
            pa.array(["Hello", "world", "!", None], type=pa.string_view()),
            id="btrim",
        ),
        pytest.param(
            col("c").trim(),
            pa.array(["Hello", "world", "!", None], type=pa.string_view()),
            id="trim",
        ),
        pytest.param(
            col("c").md5(),
            pa.array(
                [
                    "8b1a9953c4611296a827abf8c47804d7",
                    "de802497c24568d9a85d4eb8c2b6e8fe",
                    "9033e0e305f247c0c3c80d0c7848c8b3",
                    None,
                ],
                type=pa.string(),
            ),
            id="md5",
        ),
        pytest.param(
            col("c").octet_length(),
            pa.array([5, 7, 1, None], type=pa.int32()),
            id="octet_length",
        ),
        pytest.param(
            col("c").character_length(),
            pa.array([5, 7, 1, None], type=pa.int32()),
            id="character_length",
        ),
        pytest.param(
            col("c").char_length(),
            pa.array([5, 7, 1, None], type=pa.int32()),
            id="char_length",
        ),
        pytest.param(
            col("c").rtrim(),
            pa.array(["Hello", " world", "!", None], type=pa.string_view()),
            id="rtrim",
        ),
        pytest.param(
            col("c").ltrim(),
            pa.array(["Hello", "world ", "!", None], type=pa.string_view()),
            id="ltrim",
        ),
        pytest.param(
            col("c").upper(),
            pa.array(["HELLO", " WORLD ", "!", None], type=pa.string()),
            id="upper",
        ),
        pytest.param(
            lit(65).chr(),
            pa.array(["A", "A", "A", "A"], type=pa.string()),
            id="chr",
        ),
        #
        # Time Functions
        #
        pytest.param(
            col("b").from_unixtime(),
            pa.array(
                [
                    datetime(1969, 12, 31, 23, 59, 30, tzinfo=timezone.utc),
                    datetime(1970, 1, 1, 0, 0, 42, tzinfo=timezone.utc),
                    datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    None,
                ],
                type=pa.timestamp("s"),
            ),
            id="from_unixtime",
        ),
        pytest.param(
            col("c").initcap(),
            pa.array(["Hello", " World ", "!", None], type=pa.string_view()),
            id="initcap",
        ),
        #
        # Array Functions
        #
        pytest.param(
            col("d").array_pop_back(),
            pa.array([[-1, 1], [5, 10, 15], [], None], type=pa.list_(pa.int64())),
            id="array_pop_back",
        ),
        pytest.param(
            col("d").array_pop_front(),
            pa.array([[1, 0], [10, 15, 20], [], None], type=pa.list_(pa.int64())),
            id="array_pop_front",
        ),
        pytest.param(
            col("d").array_length(),
            pa.array([3, 4, 0, None], type=pa.uint64()),
            id="array_length",
        ),
        pytest.param(
            col("d").list_length(),
            pa.array([3, 4, 0, None], type=pa.uint64()),
            id="list_length",
        ),
        pytest.param(
            col("d").array_ndims(),
            pa.array([1, 1, 1, None], type=pa.uint64()),
            id="array_ndims",
        ),
        pytest.param(
            col("d").list_ndims(),
            pa.array([1, 1, 1, None], type=pa.uint64()),
            id="list_ndims",
        ),
        pytest.param(
            col("d").array_dims(),
            pa.array([[3], [4], None, None], type=pa.list_(pa.uint64())),
            id="array_dims",
        ),
        pytest.param(
            col("d").array_empty(),
            pa.array([False, False, True, None], type=pa.bool_()),
            id="array_empty",
        ),
        pytest.param(
            col("d").list_distinct(),
            pa.array(
                [[-1, 0, 1], [5, 10, 15, 20], [], None], type=pa.list_(pa.int64())
            ),
            id="list_distinct",
        ),
        pytest.param(
            col("d").array_distinct(),
            pa.array(
                [[-1, 0, 1], [5, 10, 15, 20], [], None], type=pa.list_(pa.int64())
            ),
            id="array_distinct",
        ),
        pytest.param(
            col("d").cardinality(),
            pa.array([3, 4, None, None], type=pa.uint64()),
            id="cardinality",
        ),
        pytest.param(
            col("f").flatten(),
            pa.array(
                [[-1, 1, 0, 4, 4], [5, 10, 15, 20, 3], [], []],
                type=pa.list_(pa.int64()),
            ),
            id="flatten",
        ),
        pytest.param(
            col("d").list_dims(),
            pa.array([[3], [4], None, None], type=pa.list_(pa.uint64())),
            id="list_dims",
        ),
        pytest.param(
            col("d").empty(),
            pa.array([False, False, True, None], type=pa.bool_()),
            id="empty",
        ),
        #
        # Other Tests
        #
        pytest.param(
            col("d").arrow_typeof(),
            pa.array(
                [
                    'List(Field { name: "item", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })',  # noqa: E501
                    'List(Field { name: "item", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })',  # noqa: E501
                    'List(Field { name: "item", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })',  # noqa: E501
                    'List(Field { name: "item", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })',  # noqa: E501
                ],
                type=pa.string(),
            ),
            id="arrow_typeof",
        ),
    ],
)
def test_expr_functions(ctx, function, expected_result):
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([-0.75, 0.5, 0.0, None], type=pa.float64()),
            pa.array([-30, 42, 0, None], type=pa.int64()),
            pa.array(["Hello", " world ", "!", None], type=pa.string_view()),
            pa.array(
                [[-1, 1, 0], [5, 10, 15, 20], [], None], type=pa.list_(pa.int64())
            ),
            pa.array([-0.75, float("nan"), 0.0, None], type=pa.float64()),
            pa.array(
                [[[-1, 1, 0], [4, 4]], [[5, 10, 15, 20], [3]], [[]], [None]],
                type=pa.list_(pa.list_(pa.int64())),
            ),
        ],
        names=["a", "b", "c", "d", "e", "f"],
    )
    df = ctx.create_dataframe([[batch]]).select(function)
    result = df.collect()

    assert len(result) == 1
    assert result[0].column(0).equals(expected_result)


def test_literal_metadata(ctx):
    result = (
        ctx.from_pydict({"a": [1]})
        .select(
            lit(1).alias("no_metadata"),
            lit_with_metadata(2, {"key1": "value1"}).alias("lit_with_metadata_fn"),
            literal_with_metadata(3, {"key2": "value2"}).alias(
                "literal_with_metadata_fn"
            ),
        )
        .collect()
    )

    expected_schema = pa.schema(
        [
            pa.field("no_metadata", pa.int64(), nullable=False),
            pa.field(
                "lit_with_metadata_fn",
                pa.int64(),
                nullable=False,
                metadata={"key1": "value1"},
            ),
            pa.field(
                "literal_with_metadata_fn",
                pa.int64(),
                nullable=False,
                metadata={"key2": "value2"},
            ),
        ]
    )

    expected = pa.RecordBatch.from_pydict(
        {
            "no_metadata": pa.array([1]),
            "lit_with_metadata_fn": pa.array([2]),
            "literal_with_metadata_fn": pa.array([3]),
        },
        schema=expected_schema,
    )

    assert result[0] == expected

    # Testing result[0].schema == expected_schema does not check each key/value pair
    # so we want to explicitly test these
    for expected_field in expected_schema:
        actual_field = result[0].schema.field(expected_field.name)
        assert expected_field.metadata == actual_field.metadata
