pilot_query = """
select avg (ss_net_profit) avg_1, stddev (ss_net_profit) std_1, count(*) sample_size       from store_sales {sampling_method} where ss_quantity       between 1      and 20
"""

results_mapping = [
    {"aggregate": "avg", "mean": "avg_1", "std": "std_1", "size": "sample_size"}
]

subquery_dict = []
