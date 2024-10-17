pilot_query = """
SELECT AVG(AdvEngineID) as avg_1, 
COUNT(*) AS sample_size, 
AVG(ResolutionWidth) as avg_2,
stddev(AdvEngineID) as std_1,
stddev(ResolutionWidth) as std_2
FROM hits {sampling_method};
"""

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
    {"aggregate": "count", "size": "sample_size"},
    {"aggregate": "avg", "mean": "avg_2", "std": "std_2"},
]

subquery_dict = []
