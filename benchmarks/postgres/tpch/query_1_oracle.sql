SELECT
    AVG(r2) AS avg_1,
    STDDEV(r2) AS std_1,
    AVG(r3) AS avg_2,
    STDDEV(r3) AS std_2,
    AVG(r4) AS avg_3,
    STDDEV(r4) AS std_3,
    AVG(r5) AS avg_4,
    STDDEV(r5) AS std_4,
    AVG(r8) AS avg_5,
    STDDEV(r8) AS std_5,
    AVG(r9) AS avg_6,
    STDDEV(r9) AS std_6,
    COUNT(*) AS n_page
FROM (
    SELECT 
        l_returnflag AS r0, 
        l_linestatus AS r1, 
        SUM(l_quantity) AS r2, 
        SUM(l_extendedprice) AS r3, 
        SUM(l_extendedprice * (1 - l_discount)) AS r4, 
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS r5, 
        SUM(l_discount) AS r8, 
        COUNT(*) AS r9, 
        CAST((CAST(CAST(lineitem.ctid AS TEXT) AS point))[0] AS INT) AS page_id_0 
    FROM lineitem
    WHERE l_shipdate <= CAST('1998-12-01' AS DATE) - INTERVAL '90' DAY 
    GROUP BY l_returnflag, l_linestatus, page_id_0
)
GROUP BY r0, r1;