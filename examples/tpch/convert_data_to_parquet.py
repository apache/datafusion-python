# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pyarrow
import datafusion
import os

ctx = datafusion.SessionContext()

all_schemas = {}

all_schemas["customer"] = [
    ("C_CUSTKEY", pyarrow.int32()),
    ("C_NAME", pyarrow.string()),
    ("C_ADDRESS", pyarrow.string()),
    ("C_NATIONKEY", pyarrow.int32()),
    ("C_PHONE", pyarrow.string()),
    ("C_ACCTBAL", pyarrow.float32()),
    ("C_MKTSEGMENT", pyarrow.string()),
    ("C_COMMENT", pyarrow.string()),
]

all_schemas["lineitem"] = [
    ("L_ORDERKEY", pyarrow.int32()),
    ("L_PARTKEY", pyarrow.int32()),
    ("L_SUPPKEY", pyarrow.int32()),
    ("L_LINENUMBER", pyarrow.int32()),
    ("L_QUANTITY", pyarrow.float32()),
    ("L_EXTENDEDPRICE", pyarrow.float32()),
    ("L_DISCOUNT", pyarrow.float32()),
    ("L_TAX", pyarrow.float32()),
    ("L_RETURNFLAG", pyarrow.string()),
    ("L_LINESTATUS", pyarrow.string()),
    ("L_SHIPDATE", pyarrow.date32()),
    ("L_COMMITDATE", pyarrow.date32()),
    ("L_RECEIPTDATE", pyarrow.date32()),
    ("L_SHIPINSTRUCT", pyarrow.string()),
    ("L_SHIPMODE", pyarrow.string()),
    ("L_COMMENT", pyarrow.string()),
]

all_schemas["nation"] = [
    ("N_NATIONKEY", pyarrow.int32()),
    ("N_NAME", pyarrow.string()),
    ("N_REGIONKEY", pyarrow.int32()),
    ("N_COMMENT", pyarrow.string()),
]

all_schemas["orders"] = [
    ("O_ORDERKEY", pyarrow.int32()),
    ("O_CUSTKEY", pyarrow.int32()),
    ("O_ORDERSTATUS", pyarrow.string()),
    ("O_TOTALPRICE", pyarrow.float32()),
    ("O_ORDERDATE", pyarrow.date32()),
    ("O_ORDERPRIORITY", pyarrow.string()),
    ("O_CLERK", pyarrow.string()),
    ("O_SHIPPRIORITY", pyarrow.int32()),
    ("O_COMMENT", pyarrow.string()),
]

all_schemas["part"] = [
    ("P_PARTKEY", pyarrow.int32()),
    ("P_NAME", pyarrow.string()),
    ("P_MFGR", pyarrow.string()),
    ("P_BRAND", pyarrow.string()),
    ("P_TYPE", pyarrow.string()),
    ("P_SIZE", pyarrow.int32()),
    ("P_CONTAINER", pyarrow.string()),
    ("P_RETAILPRICE", pyarrow.float32()),
    ("P_COMMENT", pyarrow.string()),
]

all_schemas["partsupp"] = [
    ("PS_PARTKEY", pyarrow.int32()),
    ("PS_SUPPKEY", pyarrow.int32()),
    ("PS_AVAILQTY", pyarrow.int32()),
    ("PS_SUPPLYCOST", pyarrow.float32()),
    ("PS_COMMENT", pyarrow.string()),
]

all_schemas["region"] = [
    ("r_REGIONKEY", pyarrow.int32()),
    ("r_NAME", pyarrow.string()),
    ("r_COMMENT", pyarrow.string()),
]

all_schemas["supplier"] = [
    ("S_SUPPKEY", pyarrow.int32()),
    ("S_NAME", pyarrow.string()),
    ("S_ADDRESS", pyarrow.string()),
    ("S_NATIONKEY", pyarrow.int32()),
    ("S_PHONE", pyarrow.string()),
    ("S_ACCTBAL", pyarrow.float32()),
    ("S_COMMENT", pyarrow.string()),
]

curr_dir = os.path.dirname(os.path.abspath(__file__))
for filename in all_schemas:
    curr_schema = all_schemas[filename]

    # For convenience, go ahead and convert the schema column names to lowercase
    curr_schema = [(s[0].lower(), s[1]) for s in curr_schema]

    # Pre-collect the output columns so we can ignore the null field we add
    # in to handle the trailing | in the file
    output_cols = [r[0] for r in curr_schema]

    # Trailing | requires extra field for in processing
    curr_schema.append(("some_null", pyarrow.null()))

    schema = pyarrow.schema(curr_schema)

    source_file = os.path.abspath(
        os.path.join(curr_dir, f"../../benchmarks/tpch/data/{filename}.csv")
    )
    dest_file = os.path.abspath(os.path.join(curr_dir, f"./data/{filename}.parquet"))

    df = ctx.read_csv(source_file, schema=schema, has_header=False, delimiter="|")

    df = df.select_columns(*output_cols)

    df.write_parquet(dest_file, compression="snappy")
