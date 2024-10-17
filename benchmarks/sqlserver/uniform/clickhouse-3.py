pilot_query = """
WITH RandomSample AS (
    SELECT AdvEngineID,
           ResolutionWidth,
           RAND(CHECKSUM(NEWID())) AS rand_value
    FROM hits
)
SELECT AVG(CAST(AdvEngineID AS BIGINT)) AS avg_1, 
       COUNT_BIG(*) AS sample_size, 
       AVG(CAST(ResolutionWidth AS BIGINT)) AS avg_2,
       STDEV(CAST(AdvEngineID AS BIGINT)) AS std_1,
       STDEV(CAST(ResolutionWidth AS BIGINT)) AS std_2
FROM RandomSample
WHERE rand_value < {sampling_method}
"""


sampling_query = """
SELECT SUM(CAST(AdvEngineID AS BIGINT)) / {sample_rate},
  COUNT_BIG(*) / {sample_rate},
  AVG(CAST(ResolutionWidth AS BIGINT))
FROM hits
where RAND(CHECKSUM(NEWID())) < {sampling_method};
"""

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
    {"aggregate": "count", "size": "sample_size"},
    {"aggregate": "avg", "mean": "avg_2", "std": "std_2"},
]

subquery_dict = []
