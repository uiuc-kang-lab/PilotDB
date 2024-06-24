pilot_query = """
select
    count_big(*) as sample_size
from
    order_products
where
    reordered = 1
    and add_to_cat_order < 5
    AND RAND(CHECKSUM(NEWID())) < {sampling_method};
"""

sampling_query = """
select
    count_big(*) / {sample_rate}  as count_product
from
    order_products
where
    reordered = 1
    and add_to_cat_order < 5
    AND RAND(CHECKSUM(NEWID())) < {sampling_method};
"""
results_mapping = [
    {"aggregate": "count", "size": "sample_size"},
]

subquery_dict = []