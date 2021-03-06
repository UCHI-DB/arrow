select cntrycode,
       count(*)       as numcust,
       sum(c_acctbal) as totacctbal
from (
         select substring(c_phone from 1 for 2) as cntrycode,
                c_acctbal
         from customer
         where substr(c_phone, 1, 2) in ('31', '13', '23', '29', '30', '18', '17')
           and c_acctbal > (
             select avg(c_acctbal)
             from customer
             where c_acctbal > 0.00
               and substr(c_phone, 1, 2) in
                   ('31', '13', '23', '29', '30', '18', '17')
         )
           and not exists(
                 select *
                 from orders
                 where o_custkey = c_custkey
             )
     ) as custsale
group by cntrycode
order by cntrycode;

SELECT last_statement_duration_us / 1000000.0 last_statement_duration_seconds
FROM current_session;