from pilotdb.pilot_engine.query_base import Query

original_query = """select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            customer left outer join orders on
                c_custkey = o_custkey
                and o_comment not like '%special%requests%'
        group by
            c_custkeys
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc;"""

pilot_query = """select
    c_count as c0,
    page_id as c1,
    count(*) as c2
from
    (
        select
            c_custkey,
            count(o_orderkey),
            {page_id}
        from
            customer left outer join orders {sample} on
                c_custkey = o_custkey
                and o_comment not like '%special%requests%'
        group by
            c_custkeys
    ) as c_orders (c_custkey, c_count, page_id)
group by
    c_count,
    page_id;"""

final_sample_query = """select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            customer left outer join orders {sample} on
                c_custkey = o_custkey
                and o_comment not like '%special%requests%'
        group by
            c_custkeys
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc;"""

column_mapping = [
    {"aggregate": "count", "page_size": "c2"},
]

page_size_col = "c2"
page_id_col = "c1"
group_cols = ["c0"]

query = Query(
    original_query=original_query,
    final_sample_query=final_sample_query,
    pilot_query=pilot_query,
    column_mapping=column_mapping,
    page_size_col=page_size_col,
    page_id_col=page_id_col,
    group_cols=group_cols
)