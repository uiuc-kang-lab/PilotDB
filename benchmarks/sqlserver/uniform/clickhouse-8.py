pilot_query = """
SELECT AdvEngineID,
  COUNT(*) AS sample_size
FROM hits {sampling_method}
WHERE AdvEngineID <> 0
GROUP BY AdvEngineID;
"""

sampling_query = '''
SELECT AdvEngineID,
  COUNT(*) / {sample_rate}
FROM hits
WHERE AdvEngineID <> 0 {sampling_method}
GROUP BY AdvEngineID
ORDER BY COUNT(*) DESC;
'''
results_mapping = [
    {"aggregate": "count", "size": "sample_size"}
]

subquery_dict = []