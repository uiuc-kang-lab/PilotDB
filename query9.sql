SELECT nation,
  o_year,
  SUM(amount) / sampling_rate AS sum_profit,
  page_id_0
FROM (
    SELECT n_name AS nation,
      EXTRACT(
        year
        FROM o_orderdate
      ) AS o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount,
      'page_id_0:' || CAST(
        (CAST(CAST(lineitem.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM part,
      supplier,
      lineitem AS sampling_method TABLESAMPLE SYSTEM (1 ROWS),
      partsupp,
      orders,
      nation
    WHERE s_suppkey = l_suppkey
      AND ps_suppkey = l_suppkey
      AND ps_partkey = l_partkey
      AND p_partkey = l_partkey
      AND o_orderkey = l_orderkey
      AND s_nationkey = n_nationkey
      AND p_name LIKE '%green%'
  ) AS profit
GROUP BY nation,
  o_year,
  page_id_0