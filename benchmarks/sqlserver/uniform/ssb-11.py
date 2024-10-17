pilot_query = """
SELECT
    d_year,
    c_nation,
    AVG(lo_revenue - lo_supplycost) AS avg_1,
    STDDEV(lo_revenue - lo_supplycost) AS std_1,
    COUNT(*) as sample_size
FROM dates, customer, supplier, part, lineorder {sampling_method}
WHERE
    lo_custkey = c_custkey
    AND lo_suppkey = s_suppkey
    AND lo_partkey = p_partkey
    AND lo_orderdate = d_datekey
    AND c_region = 'AMERICA'
    AND s_region = 'AMERICA'
    AND (
        p_mfgr = 'MFGR#1'
        OR p_mfgr = 'MFGR#2'
    )
GROUP BY d_year, c_nation;
"""

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
]

subquery_dict = []
