SELECT promotions AS r0,
  total AS r1,
  CAST(promotions AS DECIMAL(15, 4)) AS r2,
  CAST(total AS DECIMAL(15, 4)) AS r3,
  page_id_0,
  page_id_1
FROM (
    SELECT SUM(ss_ext_sales_price) AS promotions,
      'page_id_0:' || CAST(
        (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
      store,
      promotion,
      date_dim,
      customer,
      customer_address,
      item
    WHERE ss_sold_date_sk = d_date_sk
      AND ss_store_sk = s_store_sk
      AND ss_promo_sk = p_promo_sk
      AND ss_customer_sk = c_customer_sk
      AND ca_address_sk = c_current_addr_sk
      AND ss_item_sk = i_item_sk
      AND ca_gmt_offset = -7
      AND i_category = 'Jewelry'
      AND (
        p_channel_dmail = 'Y'
        OR p_channel_email = 'Y'
        OR p_channel_tv = 'Y'
      )
      AND s_gmt_offset = -7
      AND d_year = 1999
      AND d_moy = 11
    GROUP BY page_id_0
  ) AS promotional_sales,
  (
    SELECT SUM(ss_ext_sales_price) AS total,
      'page_id_1:' || CAST(
        (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_1
    FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
      store,
      date_dim,
      customer,
      customer_address,
      item
    WHERE ss_sold_date_sk = d_date_sk
      AND ss_store_sk = s_store_sk
      AND ss_customer_sk = c_customer_sk
      AND ca_address_sk = c_current_addr_sk
      AND ss_item_sk = i_item_sk
      AND ca_gmt_offset = -7
      AND i_category = 'Jewelry'
      AND s_gmt_offset = -7
      AND d_year = 1999
      AND d_moy = 11
    GROUP BY page_id_1
  ) AS all_sales