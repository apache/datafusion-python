<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# DataFusion Python Examples

Some examples rely on data which can be downloaded from the following site:

- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Here is a direct link to the file used in the examples:

- https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet

### Creating a SessionContext

- [Creating a SessionContext](./create-context.py)

### Executing Queries with DataFusion

- [Query a Parquet file using SQL](./sql-parquet.py)
- [Query a Parquet file using the DataFrame API](./dataframe-parquet.py)
- [Run a SQL query and store the results in a Pandas DataFrame](./sql-to-pandas.py)
- [Query PyArrow Data](./query-pyarrow-data.py)

### Running User-Defined Python Code

- [Register a Python UDF with DataFusion](./python-udf.py)
- [Register a Python UDAF with DataFusion](./python-udaf.py)

### Substrait Support

- [Serialize query plans using Substrait](./substrait.py)

### Executing SQL against DataFrame Libraries (Experimental)

- [Executing SQL on Polars](./sql-on-polars.py)
- [Executing SQL on Pandas](./sql-on-pandas.py)
- [Executing SQL on cuDF](./sql-on-cudf.py)

## TPC-H Examples

Within the subdirectory `tpch` there are 22 examples that reproduce queries in
the TPC-H specification. These include realistic data that can be generated at
arbitrary scale and allow the user to see use cases for a variety of data frame
operations.

In the list below we describe which new operations can be found in the examples.
The queries are designed to be of increasing complexity, so it is recommended to
review them in order. For brevity, the following list does not include operations
found in previous examples.

- [Convert CSV to Parquet](./tpch/convert_data_to_parquet.py)
    - Read from a CSV files where the delimiter is something other than a comma
    - Specify schema during CVS reading
    - Write to a parquet file
- [Pricing Summary Report](./tpch/q01_pricing_summary_report.py)
    - Aggregation computing the maximum value, average, sum, and number of entries
    - Filter data by date and interval
    - Sorting
- [Minimum Cost Supplier](./tpch/q02_minimum_cost_supplier.py)
    - Window operation to find minimum
    - Sorting in descending order
- [Shipping Priority](./tpch/q03_shipping_priority.py)
- [Order Priority Checking](./tpch/q04_order_priority_checking.py)
    - Aggregating multiple times in one data frame
- [Local Supplier Volume](./tpch/q05_local_supplier_volume.py)
- [Forecasting Revenue Change](./tpch/q06_forecasting_revenue_change.py)
    - Using collect and extracting values as a python object
- [Volume Shipping](./tpch/q07_volume_shipping.py)
    - Finding multiple distinct and mutually exclusive values within one dataframe
    - Using `case` and `when` statements
- [Market Share](./tpch/q08_market_share.py)
    - The operations in this query are similar to those in the prior examples, but
      it is a more complex example of using filters, joins, and aggregates
    - Using left outer joins
- [Product Type Profit Measure](./tpch/q09_product_type_profit_measure.py)
    - Extract year from a date
- [Returned Item Reporting](./tpch/q10_returned_item_reporting.py)
- [Important Stock Identification](./tpch/q11_important_stock_identification.py)
- [Shipping Modes and Order](./tpch/q12_ship_mode_order_priority.py)
    - Finding non-null values using a boolean operation in a filter
    - Case statement with default value
- [Customer Distribution](./tpch/q13_customer_distribution.py)
- [Promotion Effect](./tpch/q14_promotion_effect.py)
- [Top Supplier](./tpch/q15_top_supplier.py)
- [Parts/Supplier Relationship](./tpch/q16_part_supplier_relationship.py)
    - Using anti joins
    - Using regular expressions (regex)
    - Creating arrays of literal values
    - Determine if an element exists within an array
- [Small-Quantity-Order Revenue](./tpch/q17_small_quantity_order.py)
- [Large Volume Customer](./tpch/q18_large_volume_customer.py)
- [Discounted Revenue](./tpch/q19_discounted_revenue.py)
    - Creating a user defined function (UDF)
    - Convert pyarrow Array to python values
    - Filtering based on a UDF
- [Potential Part Promotion](./tpch/q20_potential_part_promotion.py)
    - Extracting part of a string using substr
- [Suppliers Who Kept Orders Waiting](./tpch/q21_suppliers_kept_orders_waiting.py)
    - Using array aggregation
    - Determining the size of array elements
- [Global Sales Opportunity](./tpch/q22_global_sales_opportunity.py)
