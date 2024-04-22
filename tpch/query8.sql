SELECT o_year AS r0,
  SUM(
    CASE
      WHEN nation = 'BRAZIL' THEN volume
      ELSE 0
    END
  ) AS r1,
  SUM(volume) AS r2,
  page_id_0
FROM (
    SELECT EXTRACT(
        year
        FROM o_orderdate
      ) AS o_year,
      l_extendedprice * (1 - l_discount) AS volume,
      n2.n_name AS nation,
      'page_id_0:' || CAST(
        (CAST(CAST(lineitem.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM part,
      supplier,
      lineitem AS sampling_method TABLESAMPLE SYSTEM (1 ROWS),
      orders,
      customer,
      nation AS n1,
      nation AS n2,
      region
    WHERE p_partkey = l_partkey
      AND s_suppkey = l_suppkey
      AND l_orderkey = o_orderkey
      AND o_custkey = c_custkey
      AND c_nationkey = n1.n_nationkey
      AND n1.n_regionkey = r_regionkey
      AND r_name = 'AMERICA'
      AND s_nationkey = n2.n_nationkey
      AND o_orderdate BETWEEN CAST('1995-01-01' AS DATE)
      AND CAST('1996-12-31' AS DATE)
      AND p_type = 'ECONOMY ANODIZED STEEL'
  ) AS all_nations
GROUP BY o_year,
  page_id_0