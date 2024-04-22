WITH ss AS (
  SELECT s_store_sk,
    SUM(ss_ext_sales_price) AS sales,
    SUM(ss_net_profit) AS profit,
    'page_id_0:' || CAST(
      (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
    ) AS page_id_0
  FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
    date_dim,
    store
  WHERE ss_sold_date_sk = d_date_sk
    AND d_date BETWEEN CAST('1998-08-05' AS DATE)
    AND (CAST('1998-08-05' AS DATE) + 30 AS days)
    AND ss_store_sk = s_store_sk
  GROUP BY s_store_sk,
    page_id_0
),
sr AS (
  SELECT s_store_sk,
    SUM(sr_return_amt) AS RETURNS,
    SUM(sr_net_loss) AS profit_loss,
    'page_id_1:' || CAST(
      (CAST(CAST(store_returns.ctid AS TEXT) AS point)) [0] AS INT
    ) AS page_id_0
  FROM store_returns TABLESAMPLE SYSTEM (1 ROWS),
    date_dim,
    store
  WHERE sr_returned_date_sk = d_date_sk
    AND d_date BETWEEN CAST('1998-08-05' AS DATE)
    AND (CAST('1998-08-05' AS DATE) + 30 AS days)
    AND sr_store_sk = s_store_sk
  GROUP BY s_store_sk,
    page_id_0
),
cs AS (
  SELECT cs_call_center_sk,
    SUM(cs_ext_sales_price) AS sales,
    SUM(cs_net_profit) AS profit,
    'page_id_2:' || CAST(
      (CAST(CAST(catalog_sales.ctid AS TEXT) AS point)) [0] AS INT
    ) AS page_id_0
  FROM catalog_sales TABLESAMPLE SYSTEM (1 ROWS),
    date_dim
  WHERE cs_sold_date_sk = d_date_sk
    AND d_date BETWEEN CAST('1998-08-05' AS DATE)
    AND (CAST('1998-08-05' AS DATE) + 30 AS days)
  GROUP BY cs_call_center_sk,
    page_id_0
),
cr AS (
  SELECT cr_call_center_sk,
    SUM(cr_return_amount) AS RETURNS,
    SUM(cr_net_loss) AS profit_loss,
    'page_id_3:' || CAST(
      (CAST(CAST(date_dim.ctid AS TEXT) AS point)) [0] AS INT
    ) AS page_id_0
  FROM catalog_returns,
    date_dim TABLESAMPLE SYSTEM (1 ROWS)
  WHERE cr_returned_date_sk = d_date_sk
    AND d_date BETWEEN CAST('1998-08-05' AS DATE)
    AND (CAST('1998-08-05' AS DATE) + 30 AS days)
  GROUP BY cr_call_center_sk,
    page_id_0
),
ws AS (
  SELECT wp_web_page_sk,
    SUM(ws_ext_sales_price) AS sales,
    SUM(ws_net_profit) AS profit,
    'page_id_4:' || CAST(
      (CAST(CAST(web_sales.ctid AS TEXT) AS point)) [0] AS INT
    ) AS page_id_0
  FROM web_sales TABLESAMPLE SYSTEM (1 ROWS),
    date_dim,
    web_page
  WHERE ws_sold_date_sk = d_date_sk
    AND d_date BETWEEN CAST('1998-08-05' AS DATE)
    AND (CAST('1998-08-05' AS DATE) + 30 AS days)
    AND ws_web_page_sk = wp_web_page_sk
  GROUP BY wp_web_page_sk,
    page_id_0
),
wr AS (
  SELECT wp_web_page_sk,
    SUM(wr_return_amt) AS RETURNS,
    SUM(wr_net_loss) AS profit_loss,
    'page_id_5:' || CAST(
      (CAST(CAST(web_returns.ctid AS TEXT) AS point)) [0] AS INT
    ) AS page_id_0
  FROM web_returns TABLESAMPLE SYSTEM (1 ROWS),
    date_dim,
    web_page
  WHERE wr_returned_date_sk = d_date_sk
    AND d_date BETWEEN CAST('1998-08-05' AS DATE)
    AND (CAST('1998-08-05' AS DATE) + 30 AS days)
    AND wr_web_page_sk = wp_web_page_sk
  GROUP BY wp_web_page_sk,
    page_id_0
)
SELECT channel,
  id,
  SUM(sales) AS sales,
  SUM(RETURNS) AS RETURNS,
  SUM(profit) AS profit,
  page_id_0
FROM (
    SELECT 'store channel' AS channel,
      ss.s_store_sk AS id,
      sales,
      COALESCE(RETURNS, 0) AS RETURNS,
      (profit - COALESCE(profit_loss, 0)) AS profit,
      page_id_0
    FROM ss
      LEFT JOIN sr ON ss.s_store_sk = sr.s_store_sk
    UNION ALL
    SELECT 'catalog channel' AS channel,
      cs_call_center_sk AS id,
      sales,
      RETURNS,
      (profit - profit_loss) AS profit,
      page_id_0
    FROM cs,
      cr
    UNION ALL
    SELECT 'web channel' AS channel,
      ws.wp_web_page_sk AS id,
      sales,
      COALESCE(RETURNS, 0) AS RETURNS,
      (profit - COALESCE(profit_loss, 0)) AS profit,
      page_id_0
    FROM ws
      LEFT JOIN wr ON ws.wp_web_page_sk = wr.wp_web_page_sk
  ) AS x
GROUP BY ROLLUP (channel, id, page_id_0)