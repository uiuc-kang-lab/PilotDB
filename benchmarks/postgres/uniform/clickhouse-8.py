pilot_query = """
SELECT AdvEngineID,
  COUNT(*) AS sample_size
FROM hits {sampling_method}
WHERE AdvEngineID <> 0
GROUP BY AdvEngineID;
"""

results_mapping = [{"aggregate": "count", "size": "sample_size"}]

subquery_dict = []
