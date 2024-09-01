pilot_query = """
SELECT AVG(ss_wholesale_cost) as avg_1, STDDEV(ss_wholesale_cost) as std_1, AVG(ss_net_profit) as avg_2, STDDEV(ss_net_profit) as std_2, COUNT(*) as sample_size FROM store_sales, store {sampling_method} WHERE ss_store_sk = s_store_sk AND s_number_employees = 287;
"""

results_mapping = [{"aggregate": "avg", "mean": "avg_1", "std": "std_1"}, {"aggregate": "count", "size": "sample_size"}]

subquery_dict = []
