pilot_query = """
select
    reordered,
    count_big(*) as sample_size
from
    order_products
where
    add_to_cat_order <= 5
    AND RAND(CHECKSUM(NEWID())) < {sampling_method}
group by
    reordered;
"""

sampling_query = """
select
    reordered,
    count(*) / {sample_rate} as count_order
from
    order_products
where
    add_to_cat_order <= 5
    AND RAND(CHECKSUM(NEWID())) < {sampling_method}
group by
    reordered
order by
    reordered;
"""
results_mapping = [
    {"aggregate": "count", "size": "sample_size"},
]

subquery_dict = []
