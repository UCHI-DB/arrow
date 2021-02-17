CREATE TABLE dates
(
    d_datekey VARCHAR(10),
    d_date VARCHAR(18),
    d_dayofweek VARCHAR(8),
    d_month VARCHAR(9),
    d_year INTEGER,
    d_yearmonthnum INTEGER,
    d_yearmonth VARCHAR(7),
    d_daynuminweek INTEGER,
    d_daynuminmonth INTEGER,
    d_daynuminyear INTEGER,
    d_monthnuminyear INTEGER,
    d_weeknuminyear INTEGER,
    d_sellingseason VARCHAR(12),
    d_lastdayinWEEKFL BOOLEAN,
    d_lastdayinmonthfl BOOLEAN,
    d_holidayfl BOOLEAN,
    d_weekdayfl BOOLEAN
) WITH (
      format='PARQUET',
      external_location = 'file:///local/hajiang/ssb/10/presto/date'
      );