pilot_query = """
WITH RandomSample AS (
    SELECT
        orders.order_hour_of_day,
        aisles.aisle,
        RAND(CHECKSUM(NEWID())) AS rand_value
    FROM
        orders
    JOIN
        order_products ON orders.order_id = order_products.order_id
    JOIN
        products ON products.product_id = order_products.product_id
    JOIN
        departments ON products.department_id = departments.department_id
    JOIN
        aisles ON products.aisle_id = aisles.aisle_id
    WHERE
        order_hour_of_day < 12
        AND department = 'meat seafood'
)
SELECT
    order_hour_of_day,
    AVG(CASE
        WHEN aisle = 'packaged meat' THEN 1.0
        ELSE 0.0
    END) AS avg_1,
    STDEV(CASE
        WHEN aisle = 'packaged meat' THEN 1.0
        ELSE 0.0
    END) AS std_1,
    COUNT_BIG(*) AS sample_size
FROM
    RandomSample
WHERE
    rand_value < {sampling_method} 
GROUP BY
    order_hour_of_day
ORDER BY
    order_hour_of_day;
"""

sampling_query = """
WITH RandomSample AS (
    SELECT
        orders.order_hour_of_day,
        aisles.aisle,
        RAND(CHECKSUM(NEWID())) AS rand_value
    FROM
        orders
    JOIN
        order_products ON orders.order_id = order_products.order_id
    JOIN
        products ON products.product_id = order_products.product_id
    JOIN
        departments ON products.department_id = departments.department_id
    JOIN
        aisles ON products.aisle_id = aisles.aisle_id
    WHERE
        order_hour_of_day < 12
        AND department = 'meat seafood'
)
SELECT
    order_hour_of_day,
    SUM(CASE
        WHEN aisle = 'packaged meat' THEN 1
        ELSE 0
    END) / COUNT(*) AS mkt_share
FROM
    RandomSample
WHERE
    rand_value < {sampling_method}
GROUP BY
    order_hour_of_day
ORDER BY
    order_hour_of_day;
"""
results_mapping = [
    {"aggregate": "div", "first_element": "avg_1", "second_element": "sample_size",  "size": "sample_size"},
]

subquery_dict = []