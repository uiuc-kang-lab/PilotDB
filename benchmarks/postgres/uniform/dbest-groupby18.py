
pilot_query = """
SELECT 
    AVG(ss_sales_price) avg_1,
    stddev(ss_sales_price) std_1,
    COUNT(*) AS sample_size
FROM 
    store_sales {sampling_method}
WHERE 2451845 <= ss_sold_date_sk AND ss_sold_date_sk < 2452051 
GROUP BY ss_store_sk
"""

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"}
]

subquery_dict = []
