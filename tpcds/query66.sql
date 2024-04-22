SELECT w_warehouse_name,
  w_warehouse_sq_ft,
  w_city,
  w_county,
  w_state,
  w_country,
  ship_carriers,
  year,
  SUM(jan_sales) AS jan_sales,
  SUM(feb_sales) AS feb_sales,
  SUM(mar_sales) AS mar_sales,
  SUM(apr_sales) AS apr_sales,
  SUM(may_sales) AS may_sales,
  SUM(jun_sales) AS jun_sales,
  SUM(jul_sales) AS jul_sales,
  SUM(aug_sales) AS aug_sales,
  SUM(sep_sales) AS sep_sales,
  SUM(oct_sales) AS oct_sales,
  SUM(nov_sales) AS nov_sales,
  SUM(dec_sales) AS dec_sales,
  SUM(jan_sales / w_warehouse_sq_ft) AS jan_sales_per_sq_foot,
  SUM(feb_sales / w_warehouse_sq_ft) AS feb_sales_per_sq_foot,
  SUM(mar_sales / w_warehouse_sq_ft) AS mar_sales_per_sq_foot,
  SUM(apr_sales / w_warehouse_sq_ft) AS apr_sales_per_sq_foot,
  SUM(may_sales / w_warehouse_sq_ft) AS may_sales_per_sq_foot,
  SUM(jun_sales / w_warehouse_sq_ft) AS jun_sales_per_sq_foot,
  SUM(jul_sales / w_warehouse_sq_ft) AS jul_sales_per_sq_foot,
  SUM(aug_sales / w_warehouse_sq_ft) AS aug_sales_per_sq_foot,
  SUM(sep_sales / w_warehouse_sq_ft) AS sep_sales_per_sq_foot,
  SUM(oct_sales / w_warehouse_sq_ft) AS oct_sales_per_sq_foot,
  SUM(nov_sales / w_warehouse_sq_ft) AS nov_sales_per_sq_foot,
  SUM(dec_sales / w_warehouse_sq_ft) AS dec_sales_per_sq_foot,
  SUM(jan_net) AS jan_net,
  SUM(feb_net) AS feb_net,
  SUM(mar_net) AS mar_net,
  SUM(apr_net) AS apr_net,
  SUM(may_net) AS may_net,
  SUM(jun_net) AS jun_net,
  SUM(jul_net) AS jul_net,
  SUM(aug_net) AS aug_net,
  SUM(sep_net) AS sep_net,
  SUM(oct_net) AS oct_net,
  SUM(nov_net) AS nov_net,
  SUM(dec_net) AS dec_net,
  page_id_0
FROM (
    SELECT w_warehouse_name,
      w_warehouse_sq_ft,
      w_city,
      w_county,
      w_state,
      w_country,
      'GREAT EASTERN' || ',' || 'LATVIAN' AS ship_carriers,
      d_year AS year,
      SUM(
        CASE
          WHEN d_moy = 1 THEN ws_ext_sales_price * ws_quantity
          ELSE 0
        END
      ) AS jan_sales,
      SUM(
        CASE
          WHEN d_moy = 2 THEN ws_ext_sales_price * ws_quantity
          ELSE 0
        END
      ) AS feb_sales,
      SUM(
        CASE
          WHEN d_moy = 3 THEN ws_ext_sales_price * ws_quantity
          ELSE 0
        END
      ) AS mar_sales,
      SUM(
        CASE
          WHEN d_moy = 4 THEN ws_ext_sales_price * ws_quantity
          ELSE 0
        END
      ) AS apr_sales,
      SUM(
        CASE
          WHEN d_moy = 5 THEN ws_ext_sales_price * ws_quantity
          ELSE 0
        END
      ) AS may_sales,
      SUM(
        CASE
          WHEN d_moy = 6 THEN ws_ext_sales_price * ws_quantity
          ELSE 0
        END
      ) AS jun_sales,
      SUM(
        CASE
          WHEN d_moy = 7 THEN ws_ext_sales_price * ws_quantity
          ELSE 0
        END
      ) AS jul_sales,
      SUM(
        CASE
          WHEN d_moy = 8 THEN ws_ext_sales_price * ws_quantity
          ELSE 0
        END
      ) AS aug_sales,
      SUM(
        CASE
          WHEN d_moy = 9 THEN ws_ext_sales_price * ws_quantity
          ELSE 0
        END
      ) AS sep_sales,
      SUM(
        CASE
          WHEN d_moy = 10 THEN ws_ext_sales_price * ws_quantity
          ELSE 0
        END
      ) AS oct_sales,
      SUM(
        CASE
          WHEN d_moy = 11 THEN ws_ext_sales_price * ws_quantity
          ELSE 0
        END
      ) AS nov_sales,
      SUM(
        CASE
          WHEN d_moy = 12 THEN ws_ext_sales_price * ws_quantity
          ELSE 0
        END
      ) AS dec_sales,
      SUM(
        CASE
          WHEN d_moy = 1 THEN ws_net_paid_inc_ship_tax * ws_quantity
          ELSE 0
        END
      ) AS jan_net,
      SUM(
        CASE
          WHEN d_moy = 2 THEN ws_net_paid_inc_ship_tax * ws_quantity
          ELSE 0
        END
      ) AS feb_net,
      SUM(
        CASE
          WHEN d_moy = 3 THEN ws_net_paid_inc_ship_tax * ws_quantity
          ELSE 0
        END
      ) AS mar_net,
      SUM(
        CASE
          WHEN d_moy = 4 THEN ws_net_paid_inc_ship_tax * ws_quantity
          ELSE 0
        END
      ) AS apr_net,
      SUM(
        CASE
          WHEN d_moy = 5 THEN ws_net_paid_inc_ship_tax * ws_quantity
          ELSE 0
        END
      ) AS may_net,
      SUM(
        CASE
          WHEN d_moy = 6 THEN ws_net_paid_inc_ship_tax * ws_quantity
          ELSE 0
        END
      ) AS jun_net,
      SUM(
        CASE
          WHEN d_moy = 7 THEN ws_net_paid_inc_ship_tax * ws_quantity
          ELSE 0
        END
      ) AS jul_net,
      SUM(
        CASE
          WHEN d_moy = 8 THEN ws_net_paid_inc_ship_tax * ws_quantity
          ELSE 0
        END
      ) AS aug_net,
      SUM(
        CASE
          WHEN d_moy = 9 THEN ws_net_paid_inc_ship_tax * ws_quantity
          ELSE 0
        END
      ) AS sep_net,
      SUM(
        CASE
          WHEN d_moy = 10 THEN ws_net_paid_inc_ship_tax * ws_quantity
          ELSE 0
        END
      ) AS oct_net,
      SUM(
        CASE
          WHEN d_moy = 11 THEN ws_net_paid_inc_ship_tax * ws_quantity
          ELSE 0
        END
      ) AS nov_net,
      SUM(
        CASE
          WHEN d_moy = 12 THEN ws_net_paid_inc_ship_tax * ws_quantity
          ELSE 0
        END
      ) AS dec_net,
      'page_id_0:' || CAST(
        (CAST(CAST(web_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM web_sales TABLESAMPLE SYSTEM (1 ROWS),
      warehouse,
      date_dim,
      time_dim,
      ship_mode
    WHERE ws_warehouse_sk = w_warehouse_sk
      AND ws_sold_date_sk = d_date_sk
      AND ws_sold_time_sk = t_time_sk
      AND ws_ship_mode_sk = sm_ship_mode_sk
      AND d_year = 1998
      AND t_time BETWEEN 48821 AND 48821 + 28800
      AND sm_carrier IN ('GREAT EASTERN', 'LATVIAN')
    GROUP BY w_warehouse_name,
      w_warehouse_sq_ft,
      w_city,
      w_county,
      w_state,
      w_country,
      d_year,
      page_id_0
    UNION ALL
    SELECT w_warehouse_name,
      w_warehouse_sq_ft,
      w_city,
      w_county,
      w_state,
      w_country,
      'GREAT EASTERN' || ',' || 'LATVIAN' AS ship_carriers,
      d_year AS year,
      SUM(
        CASE
          WHEN d_moy = 1 THEN cs_ext_list_price * cs_quantity
          ELSE 0
        END
      ) AS jan_sales,
      SUM(
        CASE
          WHEN d_moy = 2 THEN cs_ext_list_price * cs_quantity
          ELSE 0
        END
      ) AS feb_sales,
      SUM(
        CASE
          WHEN d_moy = 3 THEN cs_ext_list_price * cs_quantity
          ELSE 0
        END
      ) AS mar_sales,
      SUM(
        CASE
          WHEN d_moy = 4 THEN cs_ext_list_price * cs_quantity
          ELSE 0
        END
      ) AS apr_sales,
      SUM(
        CASE
          WHEN d_moy = 5 THEN cs_ext_list_price * cs_quantity
          ELSE 0
        END
      ) AS may_sales,
      SUM(
        CASE
          WHEN d_moy = 6 THEN cs_ext_list_price * cs_quantity
          ELSE 0
        END
      ) AS jun_sales,
      SUM(
        CASE
          WHEN d_moy = 7 THEN cs_ext_list_price * cs_quantity
          ELSE 0
        END
      ) AS jul_sales,
      SUM(
        CASE
          WHEN d_moy = 8 THEN cs_ext_list_price * cs_quantity
          ELSE 0
        END
      ) AS aug_sales,
      SUM(
        CASE
          WHEN d_moy = 9 THEN cs_ext_list_price * cs_quantity
          ELSE 0
        END
      ) AS sep_sales,
      SUM(
        CASE
          WHEN d_moy = 10 THEN cs_ext_list_price * cs_quantity
          ELSE 0
        END
      ) AS oct_sales,
      SUM(
        CASE
          WHEN d_moy = 11 THEN cs_ext_list_price * cs_quantity
          ELSE 0
        END
      ) AS nov_sales,
      SUM(
        CASE
          WHEN d_moy = 12 THEN cs_ext_list_price * cs_quantity
          ELSE 0
        END
      ) AS dec_sales,
      SUM(
        CASE
          WHEN d_moy = 1 THEN cs_net_paid_inc_ship_tax * cs_quantity
          ELSE 0
        END
      ) AS jan_net,
      SUM(
        CASE
          WHEN d_moy = 2 THEN cs_net_paid_inc_ship_tax * cs_quantity
          ELSE 0
        END
      ) AS feb_net,
      SUM(
        CASE
          WHEN d_moy = 3 THEN cs_net_paid_inc_ship_tax * cs_quantity
          ELSE 0
        END
      ) AS mar_net,
      SUM(
        CASE
          WHEN d_moy = 4 THEN cs_net_paid_inc_ship_tax * cs_quantity
          ELSE 0
        END
      ) AS apr_net,
      SUM(
        CASE
          WHEN d_moy = 5 THEN cs_net_paid_inc_ship_tax * cs_quantity
          ELSE 0
        END
      ) AS may_net,
      SUM(
        CASE
          WHEN d_moy = 6 THEN cs_net_paid_inc_ship_tax * cs_quantity
          ELSE 0
        END
      ) AS jun_net,
      SUM(
        CASE
          WHEN d_moy = 7 THEN cs_net_paid_inc_ship_tax * cs_quantity
          ELSE 0
        END
      ) AS jul_net,
      SUM(
        CASE
          WHEN d_moy = 8 THEN cs_net_paid_inc_ship_tax * cs_quantity
          ELSE 0
        END
      ) AS aug_net,
      SUM(
        CASE
          WHEN d_moy = 9 THEN cs_net_paid_inc_ship_tax * cs_quantity
          ELSE 0
        END
      ) AS sep_net,
      SUM(
        CASE
          WHEN d_moy = 10 THEN cs_net_paid_inc_ship_tax * cs_quantity
          ELSE 0
        END
      ) AS oct_net,
      SUM(
        CASE
          WHEN d_moy = 11 THEN cs_net_paid_inc_ship_tax * cs_quantity
          ELSE 0
        END
      ) AS nov_net,
      SUM(
        CASE
          WHEN d_moy = 12 THEN cs_net_paid_inc_ship_tax * cs_quantity
          ELSE 0
        END
      ) AS dec_net,
      'page_id_1:' || CAST(
        (CAST(CAST(catalog_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM catalog_sales TABLESAMPLE SYSTEM (1 ROWS),
      warehouse,
      date_dim,
      time_dim,
      ship_mode
    WHERE cs_warehouse_sk = w_warehouse_sk
      AND cs_sold_date_sk = d_date_sk
      AND cs_sold_time_sk = t_time_sk
      AND cs_ship_mode_sk = sm_ship_mode_sk
      AND d_year = 1998
      AND t_time BETWEEN 48821 AND 48821 + 28800
      AND sm_carrier IN ('GREAT EASTERN', 'LATVIAN')
    GROUP BY w_warehouse_name,
      w_warehouse_sq_ft,
      w_city,
      w_county,
      w_state,
      w_country,
      d_year,
      page_id_0
  ) AS x
GROUP BY w_warehouse_name,
  w_warehouse_sq_ft,
  w_city,
  w_county,
  w_state,
  w_country,
  ship_carriers,
  year,
  page_id_0