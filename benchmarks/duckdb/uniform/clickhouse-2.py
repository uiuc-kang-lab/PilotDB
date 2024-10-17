pilot_query = """
SELECT COUNT(*) as sample_size
FROM hits {sampling_method}
WHERE AdvEngineID <> 0;
"""

results_mapping = [{"aggregate": "count", "size": "sample_size"}]

subquery_dict = []
