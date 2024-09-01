pilot_query = """
SELECT AVG(ss_sales_price) as avg_1, STDDEV(ss_sales_price) as std_1, COUNT(*) as sample_size FROM store_sales {sampling_method} WHERE 2451782 <= ss_sold_date_sk AND ss_sold_date_sk < 2451988 GROUP BY ss_store_sk;
"""

results_mapping = [{"aggregate": "avg", "mean": "avg_1", "std": "std_1"}]

subquery_dict = []
