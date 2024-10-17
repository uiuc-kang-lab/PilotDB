pilot_query = """
select
    order_dow,
    avg(case
        when reordered = 1
            then 1.0
        else 0.0
    end) as avg_1,
    stdev(case
        when reordered = 1
            then 1.0
        else 0.0
    end) as std_1,
    avg(case
        when reordered = 0
            then 1.0
        else 0.0
    end) as avg_2,
    stdev(case
        when reordered = 0
            then 1.0
        else 0.0
    end) as std_2,
    count_big(*) as sample_size
from
    orders,
    order_products
where
    orders.order_id = order_products.order_id
    and add_to_cat_order <= 2
    and order_hour_of_day >= 12
    and order_hour_of_day < 18
    AND RAND(CHECKSUM(NEWID())) < {sampling_method}
group by
    order_dow;
"""

sampling_query = """
select
    order_dow,
    sum(case
        when reordered = 1
            then 1
        else 0
    end)  / {sample_rate}  as reordered_count,
    sum(case
        when reordered = 0
            then 1
        else 0
    end)  / {sample_rate} as non_reordered_count
from
    orders,
    order_products
where
    orders.order_id = order_products.order_id
    and add_to_cat_order <= 2
    and order_hour_of_day >= 12
    and order_hour_of_day < 18
    AND RAND(CHECKSUM(NEWID())) < {sampling_method}
group by
    order_dow
order by
    order_dow;

"""
results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_2", "std": "std_2", "size": "sample_size"},
]

subquery_dict = []
