CREATE TABLE part
(
    p_partkey   INTEGER,
    p_name      VARCHAR(22),
    p_mfgr      VARCHAR(6),
    p_category  VARCHAR(7),
    p_brand1    VARCHAR(9),
    p_color     VARCHAR(11),
    p_type      VARCHAR(25),
    p_size      INTEGER,
    p_container VARCHAR(10)
) WITH (
      format='PARQUET',
      external_location = 'file:///local/hajiang/ssb/10/presto/part'
      );