pilot_query = """
SELECT AVG(UserID) AS avg_1,
stddev(UserID) AS std_1
FROM hits {sampling_method};
"""

results_mapping = [
    {"aggregate": "avg", "mean": "avg_1", "std": "std_1"}
]

subquery_dict = []