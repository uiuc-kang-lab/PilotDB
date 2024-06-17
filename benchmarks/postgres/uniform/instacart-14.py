pilot_query = """
select
    avg(case
        when product_name like 'Organic%'
            then 1
        else 0
    end) as avg_1,
    stddev(case
        when product_name like 'Organic%'
            then 1
        else 0
    end) as std_1,
    count(*) as sample_size
from
    order_products {sampling_method},
    orders,
    products
where
    order_products.product_id = products.product_id
    and orders.order_id = order_products.order_id
    and order_hour_of_day >= 8
    and order_hour_of_day < 12;
"""

results_mapping = [
    {"aggregate": "div", "first_element": "avg_1", "second_element": "sample_size", "size": "sample_size"}
]

subquery_dict = []