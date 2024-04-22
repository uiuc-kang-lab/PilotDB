SELECT supp_nation,
  cust_nation,
  l_year,
  SUM(volume) / sampling_rate AS revenue,
  page_id_0
FROM (
    SELECT n1.n_name AS supp_nation,
      n2.n_name AS cust_nation,
      EXTRACT(
        year
        FROM l_shipdate
      ) AS l_year,
      l_extendedprice * (1 - l_discount) AS volume,
      'page_id_0:' || CAST(
        (CAST(CAST(lineitem.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM supplier,
      lineitem AS sampling_method TABLESAMPLE SYSTEM (1 ROWS),
      orders,
      customer,
      nation AS n1,
      nation AS n2
    WHERE s_suppkey = l_suppkey
      AND o_orderkey = l_orderkey
      AND c_custkey = o_custkey
      AND s_nationkey = n1.n_nationkey
      AND c_nationkey = n2.n_nationkey
      AND (
        (
          n1.n_name = 'FRANCE'
          AND n2.n_name = 'GERMANY'
        )
        OR (
          n1.n_name = 'GERMANY'
          AND n2.n_name = 'FRANCE'
        )
      )
      AND l_shipdate BETWEEN CAST('1995-01-01' AS DATE)
      AND CAST('1996-12-31' AS DATE)
  ) AS shipping
GROUP BY supp_nation,
  cust_nation,
  l_year,
  page_id_0