from pilotdb.pilot_engine.query_base import Query

original_query = """select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
    and l_quantity < 24;

"""

final_sample_query = """select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem {sample}
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
    and l_quantity < 24;

"""

pilot_query = """select
    {page_id} as c0,
    sum(l_extendedprice * l_discount) as c1,
    count(*) as c2
from
    lineitem {sample}
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
    and l_quantity < 24
group by
    {page_id}
"""

column_mapping = [
    {"aggregate": "sum", "page_sum": "c1"},
]

page_size_col = "c2"
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
