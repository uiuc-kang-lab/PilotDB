pilot_query = """
SELECT AVG(CAST(AdvEngineID AS BIGINT)) as avg_1, 
COUNT_BIG(*) AS sample_size, 
AVG(CAST(ResolutionWidth AS BIGINT)) as avg_2,
stdev(CAST(AdvEngineID AS BIGINT)) as std_1,
stdev(CAST(ResolutionWidth AS BIGINT)) as std_2
FROM hits 
WHERE {sampling_method};
"""


sampling_query = """
SELECT SUM(CAST(AdvEngineID AS BIGINT)) / {sample_rate},
  COUNT_BIG(*) / {sample_rate},
  AVG(CAST(ResolutionWidth AS BIGINT))
FROM hits
where {sampling_method};
"""

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
    {"aggregate": "count", "size": "sample_size"},
    {"aggregate": "avg", "mean": "avg_2", "std": "std_2"}
]

subquery_dict = []