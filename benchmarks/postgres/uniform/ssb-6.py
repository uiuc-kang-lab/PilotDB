pilot_query = """
SELECT AVG(lo_revenue) AS avg_1, 
    d_year, 
    p_brand,
    STDDEV(lo_revenue) AS std_1,
    COUNT(*) as sample_size
FROM lineorder {sampling_method}, dates, part, supplier
WHERE
    lo_orderdate = d_datekey
    AND lo_partkey = p_partkey
    AND lo_suppkey = s_suppkey
    AND p_brand = 'MFGR#2239'
    AND s_region = 'EUROPE'
GROUP BY d_year, p_brand;
"""

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
]

subquery_dict = []