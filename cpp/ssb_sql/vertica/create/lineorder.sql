CREATE TABLE lineorder
(
    lo_orderkey      INT,
    lo_linenumber    INT,
    lo_custkey       INT,
    lo_partkey       INT,
    lo_suppkey       INT,
    lo_orderdate     INT,
    lo_orderpriority VARBINARY(15),
    lo_shippriority  VARBINARY(1),
    lo_quantity      INT,
    lo_extendedprice FLOAT,
    lo_ordtotalprice FLOAT,
    lo_discount      FLOAT,
    lo_revenue       FLOAT,
    lo_supplycost    FLOAT,
    lo_tax           FLOAT,
    lo_commitdate    INT,
    lo_shipmode      VARBINARY(10)
);

COPY lineorder FROM '/local/hajiang/ssb/10/lineorder.tbl' DELIMITER '|' DIRECT;