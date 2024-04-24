from pilotdb.pilot_engine.query_base import Query

original_query = """select
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    part
where
    p_partkey = l_partkey
    and p_brand = 'Brand#23'
    and p_container = 'MED BOX'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem
        where
            l_partkey = p_partkey
    );
"""

pilot_query = """select
    {page_id} as c0,
    sum(l_extendedprice) as c1,
    count(*) as c2
from
    lineitem {sample},
    part
where
    p_partkey = l_partkey
    and p_brand = 'Brand#23'
    and p_container = 'MED BOX'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem
        where
            l_partkey = p_partkey
    )
group by {page_id};
"""

final_sample_query = """select
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem {sample},
    part
where
    p_partkey = l_partkey
    and p_brand = 'Brand#23'
    and p_container = 'MED BOX'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem
        where
            l_partkey = p_partkey
    );
"""

column_mapping = [
    {"aggregate": "sum", "page_sum": "c1"}
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