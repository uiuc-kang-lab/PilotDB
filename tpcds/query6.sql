SELECT a.ca_state AS state,
  COUNT(*) AS cnt,
  'page_id_0:' || CAST(
    (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM customer_address AS a,
  customer AS c,
  store_sales AS s TABLESAMPLE SYSTEM (1 ROWS),
  date_dim AS d,
  item AS i
WHERE a.ca_address_sk = c.c_current_addr_sk
  AND c.c_customer_sk = s.ss_customer_sk
  AND s.ss_sold_date_sk = d.d_date_sk
  AND s.ss_item_sk = i.i_item_sk
  AND d.d_month_seq = subquery_0
  AND i.i_current_price > 1.2 * (
    SELECT AVG(j.i_current_price)
    FROM item AS j
    WHERE j.i_category = i.i_category
  )
GROUP BY a.ca_state,
  page_id_0
HAVING COUNT(*) >= 10