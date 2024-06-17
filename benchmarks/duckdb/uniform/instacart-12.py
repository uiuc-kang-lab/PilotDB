pilot_query = """
select
    order_dow,
    avg(case
        when reordered = 1
            then 1
        else 0
    end) as avg_1,
    stddev(case
        when reordered = 1
            then 1
        else 0
    end) as std_1,
    avg(case
        when reordered = 0
            then 1
        else 0
    end) as avg_2,
    stddev(case
        when reordered = 0
            then 1
        else 0
    end) as std_2,
    count(*) as sample_size
from
    orders,
    order_products
where
    orders.order_id = order_products.order_id
    and add_to_cat_order <= 2
    and order_hour_of_day >= 12
    and order_hour_of_day < 18
group by
    order_dow;
"""

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_2", "std": "std_2", "size": "sample_size"}
]

subquery_dict = []