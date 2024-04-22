WITH ssr AS (
  SELECT s_store_id AS store_id,
    SUM(ss_ext_sales_price) AS sales,
    SUM(COALESCE(sr_return_amt, 0)) AS RETURNS,
    SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit,
    'page_id_0:' || CAST(
      (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
    ) AS page_id_0
  FROM store_sales TABLESAMPLE SYSTEM (1 ROWS)
    LEFT OUTER JOIN store_returns ON (
      ss_item_sk = sr_item_sk
      AND ss_ticket_number = sr_ticket_number
    ),
    date_dim,
    store,
    item,
    promotion
  WHERE ss_sold_date_sk = d_date_sk
    AND d_date BETWEEN CAST('1998-08-28' AS DATE)
    AND (CAST('1998-08-28' AS DATE) + 30 AS days)
    AND ss_store_sk = s_store_sk
    AND ss_item_sk = i_item_sk
    AND i_current_price > 50
    AND ss_promo_sk = p_promo_sk
    AND p_channel_tv = 'N'
  GROUP BY s_store_id,
    page_id_0
),
csr AS (
  SELECT cp_catalog_page_id AS catalog_page_id,
    SUM(cs_ext_sales_price) AS sales,
    SUM(COALESCE(cr_return_amount, 0)) AS RETURNS,
    SUM(cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit,
    'page_id_1:' || CAST(
      (CAST(CAST(catalog_sales.ctid AS TEXT) AS point)) [0] AS INT
    ) AS page_id_0
  FROM catalog_sales TABLESAMPLE SYSTEM (1 ROWS)
    LEFT OUTER JOIN catalog_returns ON (
      cs_item_sk = cr_item_sk
      AND cs_order_number = cr_order_number
    ),
    date_dim,
    catalog_page,
    item,
    promotion
  WHERE cs_sold_date_sk = d_date_sk
    AND d_date BETWEEN CAST('1998-08-28' AS DATE)
    AND (CAST('1998-08-28' AS DATE) + 30 AS days)
    AND cs_catalog_page_sk = cp_catalog_page_sk
    AND cs_item_sk = i_item_sk
    AND i_current_price > 50
    AND cs_promo_sk = p_promo_sk
    AND p_channel_tv = 'N'
  GROUP BY cp_catalog_page_id,
    page_id_0
),
wsr AS (
  SELECT web_site_id,
    SUM(ws_ext_sales_price) AS sales,
    SUM(COALESCE(wr_return_amt, 0)) AS RETURNS,
    SUM(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit,
    'page_id_2:' || CAST(
      (CAST(CAST(web_sales.ctid AS TEXT) AS point)) [0] AS INT
    ) AS page_id_0
  FROM web_sales TABLESAMPLE SYSTEM (1 ROWS)
    LEFT OUTER JOIN web_returns ON (
      ws_item_sk = wr_item_sk
      AND ws_order_number = wr_order_number
    ),
    date_dim,
    web_site,
    item,
    promotion
  WHERE ws_sold_date_sk = d_date_sk
    AND d_date BETWEEN CAST('1998-08-28' AS DATE)
    AND (CAST('1998-08-28' AS DATE) + 30 AS days)
    AND ws_web_site_sk = web_site_sk
    AND ws_item_sk = i_item_sk
    AND i_current_price > 50
    AND ws_promo_sk = p_promo_sk
    AND p_channel_tv = 'N'
  GROUP BY web_site_id,
    page_id_0
)
SELECT channel AS r0,
  id AS r1,
  SUM(sales) AS r2,
  SUM(RETURNS) AS r3,
  SUM(profit) AS r4,
  page_id_0
FROM (
    SELECT 'store channel' AS channel,
      'store' || store_id AS id,
      sales,
      RETURNS,
      profit,
      page_id_0
    FROM ssr
    UNION ALL
    SELECT 'catalog channel' AS channel,
      'catalog_page' || catalog_page_id AS id,
      sales,
      RETURNS,
      profit,
      page_id_0
    FROM csr
    UNION ALL
    SELECT 'web channel' AS channel,
      'web_site' || web_site_id AS id,
      sales,
      RETURNS,
      profit,
      page_id_0
    FROM wsr
  ) AS x
GROUP BY ROLLUP (channel, id, page_id_0)