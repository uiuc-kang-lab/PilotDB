pilot_query = """
SELECT AVG(CAST(lo_extendedprice AS BIGINT) * CAST(lo_discount AS BIGINT)) AS avg_1,
    STDDEV(CAST(lo_extendedprice AS BIGINT) * CAST(lo_discount AS BIGINT)) AS std_1,
    COUNT_BIG(*) as sample_size
FROM lineorder, 
    dates
WHERE
    lo_orderdate = d_datekey
    AND d_yearmonth = 'Jan1994'
    AND lo_discount BETWEEN 4 AND 6
    AND lo_quantity BETWEEN 26 AND 35
    AND {sampling_method};
"""

sampling_query = '''
SELECT SUM(CAST(lo_extendedprice AS BIGINT) * CAST(lo_discount AS BIGINT)) / {sample_rate} AS REVENUE
FROM lineorder, dates
WHERE
    lo_orderdate = d_datekey
    AND d_yearmonth = 'Jan1994'
    AND lo_discount BETWEEN 4 AND 6
    AND lo_quantity BETWEEN 26 AND 35
    AND {sampling_method};
'''
results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
]

subquery_dict = []