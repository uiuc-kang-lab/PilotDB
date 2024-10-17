pilot_query = """
SELECT AVG(lo_extendedprice * lo_discount) AS avg_1,
    STDDEV(lo_extendedprice * lo_discount) AS std_1,
    COUNT(*) as sample_size
FROM lineorder {sampling_method}, 
    dates
WHERE
    lo_orderdate = d_datekey
    AND d_yearmonth = 'Jan1994'
    AND lo_discount BETWEEN 4 AND 6
    AND lo_quantity BETWEEN 26 AND 35;
"""

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
]

subquery_dict = []
