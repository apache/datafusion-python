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

import pytest
from importlib import import_module
import pyarrow as pa
from datafusion import col, lit, functions as F
from util import get_answer_file


def df_selection(col_name, col_type):
    if col_type == pa.float64() or isinstance(col_type, pa.Decimal128Type):
        return F.round(col(col_name), lit(2)).alias(col_name)
    elif col_type == pa.string():
        return F.trim(col(col_name)).alias(col_name)
    else:
        return col(col_name)


def load_schema(col_name, col_type):
    if col_type == pa.int64() or col_type == pa.int32():
        return col_name, pa.string()
    elif isinstance(col_type, pa.Decimal128Type):
        return col_name, pa.float64()
    else:
        return col_name, col_type


def expected_selection(col_name, col_type):
    if col_type == pa.int64() or col_type == pa.int32():
        return F.trim(col(col_name)).cast(col_type).alias(col_name)
    elif col_type == pa.string():
        return F.trim(col(col_name)).alias(col_name)
    else:
        return col(col_name)


def selections_and_schema(original_schema):
    columns = [(c, original_schema.field(c).type) for c in original_schema.names]

    df_selections = [df_selection(c, t) for (c, t) in columns]
    expected_schema = [load_schema(c, t) for (c, t) in columns]
    expected_selections = [expected_selection(c, t) for (c, t) in columns]

    return (df_selections, expected_schema, expected_selections)


def check_q17(df):
    raw_value = float(df.collect()[0]["avg_yearly"][0].as_py())
    value = round(raw_value, 2)
    assert abs(value - 348406.05) < 0.001


@pytest.mark.parametrize(
    ("query_code", "answer_file"),
    [
        ("q01_pricing_summary_report", "q1"),
        ("q02_minimum_cost_supplier", "q2"),
        ("q03_shipping_priority", "q3"),
        ("q04_order_priority_checking", "q4"),
        ("q05_local_supplier_volume", "q5"),
        ("q06_forecasting_revenue_change", "q6"),
        ("q07_volume_shipping", "q7"),
        ("q08_market_share", "q8"),
        ("q09_product_type_profit_measure", "q9"),
        ("q10_returned_item_reporting", "q10"),
        ("q11_important_stock_identification", "q11"),
        ("q12_ship_mode_order_priority", "q12"),
        ("q13_customer_distribution", "q13"),
        ("q14_promotion_effect", "q14"),
        ("q15_top_supplier", "q15"),
        ("q16_part_supplier_relationship", "q16"),
        ("q17_small_quantity_order", "q17"),
        ("q18_large_volume_customer", "q18"),
        ("q19_discounted_revenue", "q19"),
        ("q20_potential_part_promotion", "q20"),
        ("q21_suppliers_kept_orders_waiting", "q21"),
        ("q22_global_sales_opportunity", "q22"),
    ],
)
def test_tpch_query_vs_answer_file(query_code: str, answer_file: str):
    module = import_module(query_code)
    df = module.df

    # Treat q17 as a special case. The answer file does not match the spec. Running at
    # scale factor 1, we have manually verified this result does match the expected value.
    if answer_file == "q17":
        return check_q17(df)

    (df_selections, expected_schema, expected_selections) = selections_and_schema(
        df.schema()
    )

    df = df.select(*df_selections)

    read_schema = pa.schema(expected_schema)

    df_expected = module.ctx.read_csv(
        get_answer_file(answer_file),
        schema=read_schema,
        delimiter="|",
        file_extension=".out",
    )

    df_expected = df_expected.select(*expected_selections)

    cols = list(read_schema.names)

    assert df.join(df_expected, (cols, cols), "anti").count() == 0
    assert df.count() == df_expected.count()
