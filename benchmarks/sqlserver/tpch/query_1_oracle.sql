SELECT
    AVG(r2) AS avg_1,
    STDEV(r2) AS std_1,
    AVG(r3) AS avg_2,
    STDEV(r3) AS std_2,
    AVG(r4) AS avg_3,
    STDEV(r4) AS std_3,
    AVG(r5) AS avg_4,
    STDEV(r5) AS std_4,
    AVG(r8) AS avg_5,
    STDEV(r8) AS std_5,
    AVG(r9) AS avg_6,
    STDEV(r9) AS std_6,
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
        COUNT_BIG(*) AS r9, 
        CAST(CAST(SUBSTRING(lineitem.%%physloc%%, 6, 1)
                    + SUBSTRING(lineitem.%%physloc%%, 5, 1) AS int) AS VARCHAR) 
                    + '||' + CAST(CAST(SUBSTRING(lineitem.%%physloc%%, 4, 1) 
                    + SUBSTRING(lineitem.%%physloc%%, 3, 1) + SUBSTRING(lineitem.%%physloc%%, 2, 1) 
                    + SUBSTRING(lineitem.%%physloc%%, 1, 1) AS int) AS VARCHAR) AS page_id_0 
    FROM lineitem
    WHERE l_shipdate <= DATEADD(day, -90, '1998-12-01')
    GROUP BY l_returnflag, l_linestatus, 
        CAST(CAST(SUBSTRING(lineitem.%%physloc%%, 6, 1)
                    + SUBSTRING(lineitem.%%physloc%%, 5, 1) AS int) AS VARCHAR) 
                    + '||' + CAST(CAST(SUBSTRING(lineitem.%%physloc%%, 4, 1) 
                    + SUBSTRING(lineitem.%%physloc%%, 3, 1) + SUBSTRING(lineitem.%%physloc%%, 2, 1) 
                    + SUBSTRING(lineitem.%%physloc%%, 1, 1) AS int) AS VARCHAR)

) T
GROUP BY r0, r1;