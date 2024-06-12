pilot_query = """
SELECT AVG(CAST(lo_extendedprice AS BIGINT) * CAST(lo_discount AS BIGINT)) AS avg_1,
    STDEV(CAST(lo_extendedprice AS BIGINT) * CAST(lo_discount AS BIGINT)) AS std_1,
    COUNT(*) as sample_size
FROM lineorder, 
    dates
WHERE
    lo_orderdate = d_datekey
    AND d_year = 1993
    AND lo_discount BETWEEN 1 AND 3
    AND lo_quantity < 25
    AND RAND(CHECKSUM(NEWID())) < {sampling_method};
"""

sampling_query = '''
SELECT SUM(CAST(lo_extendedprice AS BIGINT) * CAST(lo_discount AS BIGINT)) / {sample_rate} AS REVENUE
FROM lineorder, dates
WHERE
    lo_orderdate = d_datekey
    AND d_year = 1993
    AND lo_discount BETWEEN 1 AND 3
    AND lo_quantity < 25
    AND RAND(CHECKSUM(NEWID())) < {sampling_method};
'''
results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
]

subquery_dict = []