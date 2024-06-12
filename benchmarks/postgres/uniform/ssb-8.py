pilot_query = """
SELECT
    c_city,
    s_city,
    d_year,
    AVG(lo_revenue) AS avg_1,
    STDDEV(lo_revenue) AS std_1,
    COUNT(*) as sample_size
FROM customer, lineorder {sampling_method}, supplier, dates
WHERE
    lo_custkey = c_custkey
    AND lo_suppkey = s_suppkey
    AND lo_orderdate = d_datekey
    AND c_nation = 'UNITED STATES'
    AND s_nation = 'UNITED STATES'
    AND d_year >= 1992
    AND d_year <= 1997
GROUP BY c_city, s_city, d_year;
"""

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
]

subquery_dict = []