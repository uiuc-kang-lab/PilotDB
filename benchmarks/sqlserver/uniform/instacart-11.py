pilot_query = """
WITH RandomSample AS (
    SELECT
        products.product_id as product_id,
        products.product_name as product_name,
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
        order_hour_of_day >= 10
        AND order_hour_of_day < 15
        AND reordered = 1
)
SELECT
    product_id,
    product_name,
    COUNT_BIG(*) AS sample_size
FROM
    RandomSample
WHERE
    rand_value < {sampling_method} 
GROUP BY
    product_id,
    product_name;
"""

sampling_query = """
select
    products.product_id,
    count_big(*) / {sample_rate}  as value
from
    products,
    order_products,
    departments
where
    products.product_id = order_products.product_id
    and products.department_id = departments.department_id
    and department = 'breakfast'
    AND RAND(CHECKSUM(NEWID())) < {sampling_method}
group by
    products.product_id having
        count(*) > (
            select
                count(*) * 0.1
            from
                products,
                order_products,
                departments
            where
                products.product_id = order_products.product_id
                and products.department_id = departments.department_id
                and department = 'alcohol'
        )
order by
    value desc;
"""
results_mapping = [
    {"aggregate": "count", "size": "sample_size"},
]

subquery_dict = []
