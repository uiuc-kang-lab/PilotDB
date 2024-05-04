select
        cntrycode,
        count_BIG(*) as numcust,
        sum(c_acctbal) as totacctbal
from
        (
            select
                LEFT(c_phone, 2) as cntrycode,
                c_acctbal
            from
                customer
            where
                LEFT(c_phone, 2) in ('13', '31', '23', '29', '30', '18', '17')
                and c_acctbal > (
                    select
                        avg(c_acctbal)
                    from
                        customer
                    where
                        c_acctbal > 0.00
                        and LEFT(c_phone, 2) in ('13', '31', '23', '29', '30', '18', '17')
                )
                and not exists (
                    select
                        *
                    from
                        orders
                    where
                        o_custkey = c_custkey
                )
        ) as custsale
group by
        cntrycode
order by
        cntrycode;
