CREATE EXTERNAL TABLE customer (
    c_custkey INT,
    c_name VARBINARY(30),
    c_address VARBINARY(100),
    c_nationkey INT,
    c_phone VARBINARY(30),
    c_acctbal FLOAT,
    c_mktsegment VARBINARY(30),
    c_comment VARBINARY(150)
) AS COPY FROM '/local/hajiang/tpch/5/vertica/customer.parquet' PARQUET;