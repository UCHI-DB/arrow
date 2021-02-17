CREATE TABLE lineorder
(
    lo_orderkey      INTEGER,
    lo_linenumber    INTEGER,
    lo_custkey       INTEGER,
    lo_partkey       INTEGER,
    lo_suppkey       INTEGER,
    lo_orderdate     VARCHAR(10),
    lo_orderpriority VARCHAR(15),
    lo_shippriority  VARCHAR(1),
    lo_quantity      INTEGER,
    lo_extendedprice DOUBLE,
    lo_ordtotalprice DOUBLE,
    lo_discount      DOUBLE,
    lo_revenue       DOUBLE,
    lo_supplycost    DOUBLE,
    lo_tax           DOUBLE,
    lo_commitdate    VARCHAR(10),
    lo_shipmode      VARCHAR(10)
) WITH (
      format='PARQUET',
      external_location = 'file:///local/hajiang/ssb/10/presto/lineorder'
      );