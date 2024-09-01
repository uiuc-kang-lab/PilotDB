pilot_query = """
SELECT AVG(ss_sales_price) as avg_1, STDDEV(ss_sales_price) as std_1, COUNT(*) as sample_size FROM store_sales {sampling_method} WHERE 2452650 <= ss_sold_date_sk AND ss_sold_date_sk < 2452856;
"""

results_mapping = [{"aggregate": "count", "size": "sample_size"}]

subquery_dict = []
