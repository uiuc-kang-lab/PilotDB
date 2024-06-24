pilot_query = """
WITH RandomSample AS (
    SELECT
        departments.department,
        orders.order_hour_of_day,
        RAND(CHECKSUM(NEWID())) AS rand_value
    FROM
        orders
    JOIN
        order_products ON orders.order_id = order_products.order_id
    JOIN
        products ON products.product_id = order_products.product_id
    JOIN
        departments ON products.department_id = departments.department_id
    WHERE
        products.product_name LIKE '%green%'
)
SELECT
    department,
    order_hour_of_day,
    COUNT_BIG(*) AS sample_size
FROM
    RandomSample
WHERE
    rand_value < {sampling_method} 
GROUP BY
    department,
    order_hour_of_day
ORDER BY
    department,
    order_hour_of_day DESC;
"""

sampling_query = """
WITH RandomSample AS (
    SELECT
        departments.department,
        orders.order_hour_of_day,
        RAND(CHECKSUM(NEWID())) AS rand_value
    FROM
        orders
    JOIN
        order_products ON orders.order_id = order_products.order_id
    JOIN
        products ON products.product_id = order_products.product_id
    JOIN
        departments ON products.department_id = departments.department_id
    WHERE
        products.product_name LIKE '%green%'
)
SELECT
    department,
    order_hour_of_day,
    COUNT_BIG(*) / {sample_rate} AS count_orders 
FROM
    RandomSample
WHERE
    rand_value < {sampling_method} 
GROUP BY
    department,
    order_hour_of_day
ORDER BY
    department,
    order_hour_of_day DESC;

"""
results_mapping = [
    {"aggregate": "count", "size": "sample_size"},
]

subquery_dict = []