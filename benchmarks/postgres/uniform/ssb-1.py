pilot_query = """
SELECT AVG(lo_extendedprice * lo_discount) AS avg_1,
    STDDEV(lo_extendedprice * lo_discount) AS std_1,
    COUNT(*) as sample_size
FROM lineorder {sampling_method}, 
    dates
WHERE
    lo_orderdate = d_datekey
    AND d_year = 1993
    AND lo_discount BETWEEN 1 AND 3
    AND lo_quantity < 25;
"""

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
]

subquery_dict = []