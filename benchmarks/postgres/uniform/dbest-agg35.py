pilot_query = """
select avg (ss_list_price) avg_1, stddev (ss_list_price) std_1, count(*) sample_size     from store_sales {sampling_method} where ss_wholesale_cost between 60     and 80
"""

results_mapping = [
    {"aggregate": "count", "mean": "avg_1", "std": "std_1", "size": "sample_size"}
]

subquery_dict = []
