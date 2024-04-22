WITH ss AS (
        SELECT i_manufact_id,
                SUM(ss_ext_sales_price) AS total_sales,
                'page_id_0:' || CAST(
                        (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
                ) AS page_id_0
        FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
                date_dim,
                customer_address,
                item
        WHERE i_manufact_id IN subquery_0
                AND ss_item_sk = i_item_sk
                AND ss_sold_date_sk = d_date_sk
                AND d_year = 2002
                AND d_moy = 1
                AND ss_addr_sk = ca_address_sk
                AND ca_gmt_offset = -5
        GROUP BY i_manufact_id,
                page_id_0
),
cs AS (
        SELECT i_manufact_id,
                SUM(cs_ext_sales_price) AS total_sales,
                'page_id_1:' || CAST(
                        (CAST(CAST(catalog_sales.ctid AS TEXT) AS point)) [0] AS INT
                ) AS page_id_0
        FROM catalog_sales TABLESAMPLE SYSTEM (1 ROWS),
                date_dim,
                customer_address,
                item
        WHERE i_manufact_id IN subquery_1
                AND cs_item_sk = i_item_sk
                AND cs_sold_date_sk = d_date_sk
                AND d_year = 2002
                AND d_moy = 1
                AND cs_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -5
        GROUP BY i_manufact_id,
                page_id_0
),
ws AS (
        SELECT i_manufact_id,
                SUM(ws_ext_sales_price) AS total_sales,
                'page_id_2:' || CAST(
                        (CAST(CAST(web_sales.ctid AS TEXT) AS point)) [0] AS INT
                ) AS page_id_0
        FROM web_sales TABLESAMPLE SYSTEM (1 ROWS),
                date_dim,
                customer_address,
                item
        WHERE i_manufact_id IN subquery_2
                AND ws_item_sk = i_item_sk
                AND ws_sold_date_sk = d_date_sk
                AND d_year = 2002
                AND d_moy = 1
                AND ws_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -5
        GROUP BY i_manufact_id,
                page_id_0
)
SELECT i_manufact_id AS r0,
        SUM(total_sales) AS r1,
        page_id_0
FROM (
                SELECT *
                FROM ss
                UNION ALL
                SELECT *
                FROM cs
                UNION ALL
                SELECT *
                FROM ws
        ) AS tmp1
GROUP BY i_manufact_id,
        page_id_0