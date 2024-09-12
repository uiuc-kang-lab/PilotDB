
pilot_query = """
select avg (ss_quantity) avg_1, stddev (ss_quantity) std_1, count(*) sample_size          from store_sales {sampling_method} where ss_sales_price    between 100.00 and 150.00
"""

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"}
]

subquery_dict = []
