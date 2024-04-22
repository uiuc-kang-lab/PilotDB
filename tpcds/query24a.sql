WITH ssales AS (
  SELECT c_last_name,
    c_first_name,
    s_store_name,
    ca_state,
    s_state,
    i_color,
    i_current_price,
    i_manager_id,
    i_units,
    i_size,
    SUM(ss_net_profit) AS netpaid,
    'page_id_0:' || CAST(
      (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
    ) AS page_id_0
  FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
    store_returns,
    store,
    item,
    customer,
    customer_address
  WHERE ss_ticket_number = sr_ticket_number
    AND ss_item_sk = sr_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_item_sk = i_item_sk
    AND ss_store_sk = s_store_sk
    AND c_current_addr_sk = ca_address_sk
    AND c_birth_country <> UPPER(ca_country)
    AND s_zip = ca_zip
    AND s_market_id = 8
  GROUP BY c_last_name,
    c_first_name,
    s_store_name,
    ca_state,
    s_state,
    i_color,
    i_current_price,
    i_manager_id,
    i_units,
    i_size,
    page_id_0
)
SELECT c_last_name,
  c_first_name,
  s_store_name,
  SUM(netpaid) AS paid,
  page_id_0
FROM ssales
WHERE i_color = 'beige'
GROUP BY c_last_name,
  c_first_name,
  s_store_name,
  page_id_0
HAVING SUM(netpaid) > (
    SELECT 0.05 * AVG(netpaid)
    FROM ssales
  )