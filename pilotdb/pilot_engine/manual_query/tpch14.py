from pilotdb.pilot_engine.query_base import Query

original_query = """select
    100.00 * sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    lineitem,
    part
where
    l_partkey = p_partkey
    and l_shipdate >= date '1995-09-01'
    and l_shipdate < date '1995-09-01' + interval '1' month;
"""

pilot_query = """select
    {page_id} as c0,
    sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) as c1,
    sum(l_extendedprice * (1 - l_discount)) as c2,
    count(*) as c3
from
    lineitem {sample},
    part
where
    l_partkey = p_partkey
    and l_shipdate >= date '1995-09-01'
    and l_shipdate < date '1995-09-01' + interval '1' month
group by {page_id};
"""

final_sample_query = """select
    100.00 * sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    lineitem {sample},
    part
where
    l_partkey = p_partkey
    and l_shipdate >= date '1995-09-01'
    and l_shipdate < date '1995-09-01' + interval '1' month;
"""

column_mapping = [
    {"aggregate": "sum", "page_sum": "c1"},
    {"aggregate": "sum", "page_sum": "c2"}
]

page_size_col = "c3"
page_id_col = "c0"
group_cols = []

query = Query(
    original_query=original_query,
    final_sample_query=final_sample_query,
    pilot_query=pilot_query,
    column_mapping=column_mapping,
    page_size_col=page_size_col,
    page_id_col=page_id_col,
    group_cols=group_cols
)