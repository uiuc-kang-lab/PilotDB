pilot_query = """
WITH RandomSample AS (
    SELECT
        department,
        orders.order_hour_of_day AS order_hour_of_day,
        RAND(CHECKSUM(NEWID())) AS rand_value
    FROM
        aisles
    JOIN
        products ON aisles.aisle_id = products.aisle_id
    JOIN
        order_products ON products.product_id = order_products.product_id
    JOIN
        orders ON orders.order_id = order_products.order_id
    JOIN
        departments ON departments.department_id = products.department_id
    WHERE
        (department = 'international' OR department = 'meat seafood')
        AND order_hour_of_day >= 15
        AND order_hour_of_day <= 18
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
    order_hour_of_day;
"""

sampling_query = """
WITH RandomSample AS (
    SELECT
        department,
        orders.order_hour_of_day AS order_hour_of_day,
        RAND(CHECKSUM(NEWID())) AS rand_value
    FROM
        aisles
    JOIN
        products ON aisles.aisle_id = products.aisle_id
    JOIN
        order_products ON products.product_id = order_products.product_id
    JOIN
        orders ON orders.order_id = order_products.order_id
    JOIN
        departments ON departments.department_id = products.department_id
    WHERE
        (department = 'international' OR department = 'meat seafood')
        AND order_hour_of_day >= 15
        AND order_hour_of_day <= 18
)
SELECT
    department,
    order_hour_of_day,
    COUNT_BIG(*) / {sample_rate} AS revenue
FROM
    RandomSample
WHERE
    rand_value < {sampling_method}
GROUP BY
    department,
    order_hour_of_day
ORDER BY
    department,
    order_hour_of_day;
"""
results_mapping = [
    {"aggregate": "count", "size": "sample_size"},
]

subquery_dict = []