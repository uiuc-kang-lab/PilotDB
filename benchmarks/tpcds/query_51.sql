WITH web_v1 AS (
  SELECT ws_item_sk item_sk,
    d_date,
    sum(sum(ws_sales_price)) over (
      PARTITION by ws_item_sk
      ORDER BY d_date ROWS BETWEEN unbounded preceding AND current ROW
    ) cume_sales
  FROM web_sales,
    date_dim
  WHERE ws_sold_date_sk = d_date_sk
    AND d_month_seq BETWEEN 1216 AND 1216 + 11
    AND ws_item_sk IS NOT NULL
  GROUP BY ws_item_sk,
    d_date
),
store_v1 AS (
  SELECT ss_item_sk item_sk,
    d_date,
    sum(sum(ss_sales_price)) over (
      PARTITION by ss_item_sk
      ORDER BY d_date ROWS BETWEEN unbounded preceding AND current ROW
    ) cume_sales
  FROM store_sales,
    date_dim
  WHERE ss_sold_date_sk = d_date_sk
    AND d_month_seq BETWEEN 1216 AND 1216 + 11
    AND ss_item_sk IS NOT NULL
  GROUP BY ss_item_sk,
    d_date
)
SELECT *
FROM (
    SELECT item_sk,
      d_date,
      web_sales,
      store_sales,
      max(web_sales) over (
        PARTITION by item_sk
        ORDER BY d_date ROWS BETWEEN unbounded preceding AND current ROW
      ) web_cumulative,
      max(store_sales) over (
        PARTITION by item_sk
        ORDER BY d_date ROWS BETWEEN unbounded preceding AND current ROW
      ) store_cumulative
    FROM (
        SELECT CASE
            WHEN web.item_sk IS NOT NULL THEN web.item_sk
            ELSE store.item_sk
          END item_sk,
CASE
            WHEN web.d_date IS NOT NULL THEN web.d_date
            ELSE store.d_date
          END d_date,
          web.cume_sales web_sales,
          store.cume_sales store_sales
        FROM web_v1 web
          FULL OUTER JOIN store_v1 store ON (
            web.item_sk = store.item_sk
            AND web.d_date = store.d_date
          )
      ) x
  ) y
WHERE web_cumulative > store_cumulative
ORDER BY item_sk,
  d_date
LIMIT 100;