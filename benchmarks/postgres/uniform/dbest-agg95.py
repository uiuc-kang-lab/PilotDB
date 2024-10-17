pilot_query = """
select avg (ws_quantity) avg_1, stddev (ws_quantity) std_1, count(*) sample_size         from web_sales {sampling_method} where ws_sales_price      between 100.00 and 150.00
"""

results_mapping = [
    {"aggregate": "avg", "mean": "avg_1", "std": "std_1", "size": "sample_size"}
]

subquery_dict = []
