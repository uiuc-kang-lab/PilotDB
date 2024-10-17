pilot_query = """
SELECT 
    AVG(l_quantity) avg_1,
    stddev(l_quantity) std_1,
    COUNT(*) AS sample_size
FROM 
    lineitem {sampling_method};
"""

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"}
]

subquery_dict = []
