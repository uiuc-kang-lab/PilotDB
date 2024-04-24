from pilotdb.pilot_engine.query_base import Query
original_query = """select
        cntrycode,
        count(*) as numcust,
        sum(c_acctbal) as totacctbal
from
        (
            select
                substring(c_phone from 1 for 2) as cntrycode,
                c_acctbal
            from
                customer
            where
                substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17')
                and c_acctbal > (
                    select
                        avg(c_acctbal)
                    from
                        customer
                    where
                        c_acctbal > 0.00
                        and substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17')
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
"""

pilot_query = """select
        cntrycode as c0,
        page_id as c1,
        count(*) as c2,
        sum(c_acctbal) as c3
from
        (
            select
                substring(c_phone from 1 for 2) as cntrycode,
                c_acctbal,
                {page_id} as page_id
            from
                customer {sample}
            where
                substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17')
                and c_acctbal > (
                    select
                        avg(c_acctbal)
                    from
                        customer
                    where
                        c_acctbal > 0.00
                        and substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17')
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
        cntrycode,
        page_id;
"""

final_sample_query = """select
        cntrycode,
        count(*) as numcust,
        sum(c_acctbal) as totacctbal
from
        (
            select
                substring(c_phone from 1 for 2) as cntrycode,
                c_acctbal
            from
                customer {sample}
            where
                substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17')
                and c_acctbal > (
                    select
                        avg(c_acctbal)
                    from
                        customer
                    where
                        c_acctbal > 0.00
                        and substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17')
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
"""

column_mapping = [
    {"aggregate": "count", "page_size": "c2"},
    {"aggregate": "sum", "page_sum": "c3"}
]

page_size_col = "c2"
page_id_col = "c1"
group_cols = ["c0"]

query = Query(
    original_query=original_query,
    pilot_query=pilot_query,
    final_sample_query=final_sample_query,
    column_mapping=column_mapping,
    page_size_col=page_size_col,
    page_id_col=page_id_col,
    group_cols=group_cols
)