pilot_query = """
SELECT AVG(CAST(UserID AS DECIMAL(38, 0))) AS avg_1,
stdev(CAST(UserID AS DECIMAL(38, 0))) AS std_1
FROM hits 
WHERE {sampling_method};
"""

sampling_query = """
SELECT AVG(CAST(UserID AS DECIMAL(38, 0)))
FROM hits
WHERE {sampling_method};
"""
results_mapping = [
    {"aggregate": "avg", "mean": "avg_1", "std": "std_1"}
]

subquery_dict = []