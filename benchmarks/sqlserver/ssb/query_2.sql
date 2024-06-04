SELECT SUM(CAST(lo_extendedprice AS BIGINT) * CAST(lo_discount AS BIGINT)) AS REVENUE
FROM lineorder, dates
WHERE
    lo_orderdate = d_datekey
    AND d_yearmonth = 'Jan1994'
    AND lo_discount BETWEEN 4 AND 6
    AND lo_quantity BETWEEN 26 AND 35;