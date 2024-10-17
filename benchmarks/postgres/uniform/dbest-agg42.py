
pilot_query = """
select avg (ss_ext_sales_price) avg_1, stddev (ss_ext_sales_price) std_1, count(*) sample_size  from store_sales {sampling_method} where ss_quantity       between 21     and 40
"""

results_mapping = [
    {"aggregate": "avg", "mean": "avg_1", "std": "std_1", "size": "sample_size"}
]

subquery_dict = []
