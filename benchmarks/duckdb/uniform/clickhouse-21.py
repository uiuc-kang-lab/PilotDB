pilot_query = """
SELECT COUNT(*) AS sample_size
FROM hits{sampling_method}
WHERE URL LIKE '%google%';
"""

results_mapping = [
     {"aggregate": "count", "size": "sample_size"}
]

subquery_dict = []