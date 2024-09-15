
pilot_query = """
SELECT 
    AVG(ss_net_profit) avg_1,
    stddev(ss_net_profit) std_1,
    COUNT(*) AS sample_size
FROM 
    store_sales {sampling_method},
    store
WHERE ss_store_sk = s_store_sk AND s_number_employees = 269
"""

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"}
]

subquery_dict = []
