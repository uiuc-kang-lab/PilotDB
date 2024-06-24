pilot_query = """
select
    avg(case
        when product_name like 'Organic%'
            then 1.0
        else 0.0
    end) as avg_1,
    stdev(case
        when product_name like 'Organic%'
            then 1.0
        else 0.0
    end) as std_1,
    count_big(*) as sample_size
from
    order_products,
    orders,
    products
where
    order_products.product_id = products.product_id
    and orders.order_id = order_products.order_id
    and order_hour_of_day >= 8
    and order_hour_of_day < 12
    AND RAND(CHECKSUM(NEWID())) < {sampling_method};
"""

sampling_query = """
select
    100.00 * sum(case
        when product_name like 'Organic%'
            then 1
        else 0
    end) / count_big(*) as organic_count
from
    order_products,
    orders,
    products
where
    order_products.product_id = products.product_id
    and orders.order_id = order_products.order_id
    and order_hour_of_day >= 8
    and order_hour_of_day < 12
    AND RAND(CHECKSUM(NEWID())) < {sampling_method};
"""
results_mapping = [
    {"aggregate": "div", "first_element": "avg_1", "second_element": "sample_size", "size": "sample_size"}
]

subquery_dict = []