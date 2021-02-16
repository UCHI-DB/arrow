CREATE TABLE part
(
    p_partkey   INT,
    p_name      VARBINARY(22),
    p_mfgr      VARBINARY(6),
    p_category  VARBINARY(7),
    p_brand1    VARBINARY(9),
    p_color     VARBINARY(11),
    p_type      VARBINARY(25),
    p_size      INT,
    p_container VARBINARY(10)
);

COPY part FROM '/local/hajiang/ssb/10/part.tbl' DELIMITER '|' DIRECT;