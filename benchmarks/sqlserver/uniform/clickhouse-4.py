pilot_query = """
WITH RandomSample AS (
    SELECT UserID,
           RAND(CHECKSUM(NEWID())) AS rand_value
    FROM hits
)
SELECT AVG(CAST(UserID AS DECIMAL(38, 0))) AS avg_1,
       STDEV(CAST(UserID AS DECIMAL(38, 0))) AS std_1
FROM RandomSample
WHERE rand_value < {sampling_method};
"""

sampling_query = """
SELECT AVG(CAST(UserID AS DECIMAL(38, 0)))
FROM hits
WHERE RAND(CHECKSUM(NEWID())) < {sampling_method};
"""
results_mapping = [
    {"aggregate": "avg", "mean": "avg_1", "std": "std_1"}
]

subquery_dict = []