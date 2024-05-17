import os
import pyarrow as pa
from datafusion import col, lit, functions as F, Expr

os.environ["TPCH_USE_REF_DATA"] = "TRUE"

def compare_decimal(value, expected) -> bool:
    return abs(value.cast(pa.float64()).as_py() - expected) < 0.01

def round_decimal(col_name: str) -> Expr:
    return F.round(col(col_name), lit(2)).alias(col_name)

def test_q01_pricing_summary_report():
    import q01_pricing_summary_report
    df = q01_pricing_summary_report.df

    df = df.select(
        col("l_returnflag"),
        col("l_linestatus"),
        round_decimal("sum_qty"),
        round_decimal("sum_base_price"),
        round_decimal("sum_disc_price"),
        round_decimal("sum_charge"),
        round_decimal("avg_qty"),
        round_decimal("avg_price"),
        round_decimal("avg_disc"),
        col("count_order"),
    )

    expected_result = {
        "l_returnflag": ["A", "N", "N", "R"],
        "l_linestatus": ["F", "F", "O", "F"],
        "sum_qty": [117597.0, 2923.0, 220142.0, 114701.0],
        "sum_base_price": [177256208.31, 4510302.73, 329823621.22, 171489097.87],
        "sum_disc_price": [168202735.21, 4305696.73, 313366609.14, 162841193.55],
        "sum_charge": [174926201.01, 4493715.13, 325821079.94, 169372092.98],
        "avg_qty": [25.43, 23.57, 25.47, 25.2],
        "avg_price": [38325.67, 36373.41, 38160.78, 37681.63],
        "avg_disc": [0.05, 0.05, 0.05, 0.05],
        "count_order": [4625, 124, 8643, 4551],
    }

    cols = list(expected_result.keys())
    df_expected = q01_pricing_summary_report.ctx.from_pydict(expected_result)

    assert df.join(df_expected, (cols, cols), "anti").count() == 0
    assert df.count() == df_expected.count()


def test_q02_minimum_cost_supplier():
    import q02_minimum_cost_supplier
    df = q02_minimum_cost_supplier.df

    df = df.select(
        round_decimal("s_acctbal"),
        col("s_name"),
        col("n_name"),
        col("p_partkey"),
        col("p_mfgr"),
        col("s_address"),
        col("s_phone"),
        col("s_comment"),
    )

    expected_result = {
        "s_acctbal": [9198.31],
        "s_name": ["Supplier#000000025"],
        "n_name": ["RUSSIA"],
        "p_partkey": [24],
        "p_mfgr": ["Manufacturer#5"],
        "s_address": ["RCQKONXMFnrodzz6w7fObFVV6CUm2q"],
        "s_phone": ["32-431-945-3541"],
        "s_comment": ["ely regular deposits. carefully regular sauternes engage furiously above the regular accounts. idly "],
    }

    cols = list(expected_result.keys())
    df_expected = q02_minimum_cost_supplier.ctx.from_pydict(expected_result)

    assert df.join(df_expected, (cols, cols), "anti").count() == 0
    assert df.count() == df_expected.count()


def test_q03_shipping_priority():
    import q03_shipping_priority
    df = q03_shipping_priority.df

    df = df.select(
        col("l_orderkey"),
        round_decimal("revenue"),
        col("o_orderdate"),
        col("o_shippriority")
    )

    expected_result = {
        "l_orderkey": [5417],
        "revenue": [10396.08],
        "o_orderdate": ["1993-08-24"],
        "o_shippriority": [0],
    }

    cols = list(expected_result.keys())
    df_expected = q03_shipping_priority.ctx.from_pydict(expected_result)

    assert df.join(df_expected, (cols, cols), "anti").count() == 0
    assert df.count() == df_expected.count()

test_q03_shipping_priority()