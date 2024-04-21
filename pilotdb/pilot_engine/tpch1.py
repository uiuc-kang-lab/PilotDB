query = """select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '90 day'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;
"""

pilot_query = """select
    l_returnflag as c0,
    l_linestatus as c1,
    {group_id} as c2,
    sum(l_quantity) as c3,
    sum(l_extendedprice) as c4,
    sum(l_extendedprice * (1 - l_discount)) as c5,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as c6,
    sum(l_discount) as c7,
    count(*) as c8
from
    lineitem {sample}
where
    l_shipdate <= date '1998-12-01' - interval '90 day'
group by
    l_returnflag,
    l_linestatus,
    {group_id}
order by
    l_returnflag,
    l_linestatus;
"""

column_mapping = [
    {"aggregate": "sum", "page_sum": "c3"},
    {"aggregate": "sum", "page_sum": "c4"},
    {"aggregate": "sum", "page_sum": "c5"},
    {"aggregate": "sum", "page_sum": "c6"},
    {"aggregate": "avg", "page_sum": "c3"},
    {"aggregate": "avg", "page_sum": "c4"},
    {"aggregate": "avg", "page_sum": "c7"},
    {"aggregate": "count", "page_size": "c8"}
]
page_size_col = "c8"
group_id_col = "c2"

from pilotdb.pilot_engine.utils import aggregate_error_to_page_error

page_errors = aggregate_error_to_page_error(column_mapping, page_size_col)
print(page_errors)
