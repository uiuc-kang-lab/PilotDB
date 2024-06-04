SELECT
    SUM(CAST(lo_extendedprice AS BIGINT) * CAST(lo_discount AS BIGINT)) AS REVENUE
FROM lineorder, dates
WHERE
    lo_orderdate = d_datekey
    AND d_weeknuminyear = 6
    AND d_year = 1994
    AND lo_discount BETWEEN 5 AND 7
    AND lo_quantity BETWEEN 26 AND 35;