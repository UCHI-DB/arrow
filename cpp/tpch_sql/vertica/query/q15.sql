create view revenue as
select l_suppkey                               supplier_no,
       sum(l_extendedprice * (1 - l_discount)) total_revenue
from lineitem
where l_shipdate >= '1996-01-01'
  and l_shipdate < '1996-04-01'
group by l_suppkey;

select s_suppkey,
       s_name,
       s_address,
       s_phone,
       total_revenue
from supplier,
     revenue
where s_suppkey = supplier_no
  and total_revenue = (
    select max(total_revenue)
    from revenue
)
order by s_suppkey;
SELECT last_statement_duration_us / 1000000.0 last_statement_duration_seconds
FROM current_session;
drop view revenue;
