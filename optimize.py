from sqlglot.optimizer import optimize
import sqlglot
print(
    optimize(
        sqlglot.parse_one("""
            select
    o_orderpriority,
    count(*) /sampling_rate as order_count
from
    orders sampling_method
where
    o_orderdate >= date '1993-07-01'
    and o_orderdate < date '1993-07-01' + interval '3' month
    and exists (
        select
            *
        from
            lineitem
        where
            l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority;
        """)
    ).sql(pretty=True)
)