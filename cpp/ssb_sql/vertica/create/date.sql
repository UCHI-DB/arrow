CREATE TABLE dates
(
    d_datekey VARBINARY(10),
    d_date VARBINARY(18),
    d_dayofweek VARBINARY(8),
    d_month VARBINARY(9),
    d_year INT,
    d_yearmonthnum INT,
    d_yearmonth VARBINARY(7),
    d_daynuminweek INT,
    d_daynuminmonth INT,
    d_daynuminyear INT,
    d_monthnuminyear INT,
    d_weeknuminyear INT,
    d_sellingseason VARBINARY(12),
    d_lastdayinWEEKFL BOOLEAN,
    d_lastdayinmonthfl BOOLEAN,
    d_holidayfl BOOLEAN,
    d_weekdayfl BOOLEAN
);

COPY dates FROM '/local/hajiang/ssb/10/date.tbl' DELIMITER '|' DIRECT;