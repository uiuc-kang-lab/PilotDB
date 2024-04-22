SELECT cntrycode,
    COUNT(*) / sampling_rate AS numcust,
    SUM(c_acctbal) / sampling_rate AS totacctbal,
    page_id_0
FROM (
        SELECT SUBSTRING(c_phone, 1, 2) AS cntrycode,
            c_acctbal,
            'page_id_0:' || CAST(
                (CAST(CAST(lineitem.ctid AS TEXT) AS point)) [0] AS INT
            ) AS page_id_0
        FROM customer AS sampling_method TABLESAMPLE SYSTEM (1 ROWS)
        WHERE SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
            AND c_acctbal > subquery_0
            AND NOT EXISTS(
                SELECT *
                FROM orders
                WHERE o_custkey = c_custkey
            )
    ) AS custsale
GROUP BY cntrycode,
    page_id_0