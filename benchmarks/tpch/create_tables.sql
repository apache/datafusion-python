-- Schema derived from TPC-H schema under the terms of the TPC Fair Use Policy.
-- TPC-H queries are Copyright 1993-2022 Transaction Processing Performance Council.

CREATE EXTERNAL TABLE customer (
    c_custkey INT NOT NULL,
    c_name VARCHAR NOT NULL,
    c_address VARCHAR NOT NULL,
    c_nationkey INT NOT NULL,
    c_phone VARCHAR NOT NULL,
    c_acctbal DECIMAL(15, 2) NOT NULL,
    c_mktsegment VARCHAR NOT NULL,
    c_comment VARCHAR NOT NULL,
    c_extra VARCHAR NOT NULL,
)
STORED AS CSV
OPTIONS (
    format.delimiter '|',
    format.has_header true
)
LOCATION '$PATH/customer.csv';

CREATE EXTERNAL TABLE lineitem (
    l_orderkey INT NOT NULL,
    l_partkey INT NOT NULL,
    l_suppkey INT NOT NULL,
    l_linenumber INT NOT NULL,
    l_quantity DECIMAL(15, 2) NOT NULL,
    l_extendedprice DECIMAL(15, 2) NOT NULL,
    l_discount DECIMAL(15, 2) NOT NULL,
    l_tax DECIMAL(15, 2) NOT NULL,
    l_returnflag VARCHAR NOT NULL,
    l_linestatus VARCHAR NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct VARCHAR NOT NULL,
    l_shipmode VARCHAR NOT NULL,
    l_comment VARCHAR NOT NULL,
    l_extra VARCHAR NOT NULL,
)
STORED AS CSV
OPTIONS (
    format.delimiter '|',
    format.has_header true
)
LOCATION '$PATH/lineitem.csv';

CREATE EXTERNAL TABLE nation (
    n_nationkey INT NOT NULL,
    n_name VARCHAR NOT NULL,
    n_regionkey INT NOT NULL,
    n_comment VARCHAR NOT NULL,
    n_extra VARCHAR NOT NULL,
)
STORED AS CSV
OPTIONS (
    format.delimiter '|',
    format.has_header true
)
LOCATION '$PATH/nation.csv';

CREATE EXTERNAL TABLE orders (
    o_orderkey INT NOT NULL,
    o_custkey INT NOT NULL,
    o_orderstatus VARCHAR NOT NULL,
    o_totalprice DECIMAL(15, 2) NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority VARCHAR NOT NULL,
    o_clerk VARCHAR NOT NULL,
    o_shippriority INT NULL,
    o_comment VARCHAR NOT NULL,
    o_extra VARCHAR NOT NULL,
)
STORED AS CSV
OPTIONS (
    format.delimiter '|',
    format.has_header true
)
LOCATION '$PATH/orders.csv';

CREATE EXTERNAL TABLE part (
    p_partkey INT NOT NULL,
    p_name VARCHAR NOT NULL,
    p_mfgr VARCHAR NOT NULL,
    p_brand VARCHAR NOT NULL,
    p_type VARCHAR NOT NULL,
    p_size INT NULL,
    p_container VARCHAR NOT NULL,
    p_retailprice DECIMAL(15, 2) NOT NULL,
    p_comment VARCHAR NOT NULL,
    p_extra VARCHAR NOT NULL,
)
STORED AS CSV
OPTIONS (
    format.delimiter '|',
    format.has_header true
)
LOCATION '$PATH/part.csv';

CREATE EXTERNAL TABLE partsupp (
    ps_partkey INT NOT NULL,
    ps_suppkey INT NOT NULL,
    ps_availqty INT NOT NULL,
    ps_supplycost DECIMAL(15, 2) NOT NULL,
    ps_comment VARCHAR NOT NULL,
    ps_extra VARCHAR NOT NULL,
)
STORED AS CSV
OPTIONS (
    format.delimiter '|',
    format.has_header true
)
LOCATION '$PATH/partsupp.csv';

CREATE EXTERNAL TABLE region (
    r_regionkey INT NOT NULL,
    r_name VARCHAR NOT NULL,
    r_comment VARCHAR NOT NULL,
    r_extra VARCHAR NOT NULL,
)
STORED AS CSV
OPTIONS (
    format.delimiter '|',
    format.has_header true
)
LOCATION '$PATH/region.csv';

CREATE EXTERNAL TABLE supplier (
    s_suppkey INT NOT NULL,
    s_name VARCHAR NOT NULL,
    s_address VARCHAR NOT NULL,
    s_nationkey INT NOT NULL,
    s_phone VARCHAR NOT NULL,
    s_acctbal DECIMAL(15, 2) NOT NULL,
    s_comment VARCHAR NOT NULL,
    s_extra VARCHAR NOT NULL,
)
STORED AS CSV
OPTIONS (
    format.delimiter '|',
    format.has_header true
)
LOCATION '$PATH/supplier.csv';