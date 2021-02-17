CREATE TABLE customer
(
    c_custkey    INTEGER,
    c_name       VARCHAR(30),
    c_address    VARCHAR(30),
    c_city       VARCHAR(10),
    c_nation     VARCHAR(15),
    c_region     VARCHAR(12),
    c_phone      VARCHAR(15),
    c_mktsegment VARCHAR(10)
) WITH (
      format='PARQUET',
      external_location = 'file:///local/hajiang/ssb/10/presto/customer'
      );