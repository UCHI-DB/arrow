CREATE TABLE supplier
(
    s_suppkey INT,
    s_name    VARBINARY(25),
    s_address VARBINARY(25),
    s_city    VARBINARY(10),
    s_nation  VARBINARY(15),
    s_region  VARBINARY(12),
    s_phone   VARBINARY(15)
);

COPY supplier FROM '/local/hajiang/ssb/10/supplier.tbl' DELIMITER '|' DIRECT;