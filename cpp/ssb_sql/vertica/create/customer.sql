CREATE TABLE customer
(
    c_custkey    INT,
    c_name       VARBINARY(30),
    c_address    VARBINARY(30),
    c_city       VARBINARY(10),
    c_nation     VARBINARY(15),
    c_region     VARBINARY(12),
    c_phone      VARBINARY(15),
    c_mktsegment VARBINARY(10)
);

COPY customer FROM '/local/hajiang/ssb/10/customer.tbl' DELIMITER '|' DIRECT;