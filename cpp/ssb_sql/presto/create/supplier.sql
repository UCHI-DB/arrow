CREATE TABLE supplier
(
    s_suppkey INTEGER,
    s_name    VARCHAR(25),
    s_address VARCHAR(25),
    s_city    VARCHAR(10),
    s_nation  VARCHAR(15),
    s_region  VARCHAR(12),
    s_phone   VARCHAR(15)
) WITH (
      format='PARQUET',
      external_location = 'file:///local/hajiang/ssb/10/presto/supplier'
      );
