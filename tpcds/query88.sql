SELECT h8_30_to_9 AS r0,
  h9_to_9_30 AS r1,
  h9_30_to_10 AS r2,
  h10_to_10_30 AS r3,
  h10_30_to_11 AS r4,
  h11_to_11_30 AS r5,
  h11_30_to_12 AS r6,
  h12_to_12_30 AS r7,
  page_id_0,
  page_id_1,
  page_id_2,
  page_id_3,
  page_id_4,
  page_id_5,
  page_id_6,
  page_id_7
FROM (
    SELECT COUNT(*) AS h8_30_to_9,
      'page_id_0:' || CAST(
        (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
      household_demographics,
      time_dim,
      store
    WHERE ss_sold_time_sk = time_dim.t_time_sk
      AND ss_hdemo_sk = household_demographics.hd_demo_sk
      AND ss_store_sk = s_store_sk
      AND time_dim.t_hour = 8
      AND time_dim.t_minute >= 30
      AND (
        (
          household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2
        )
        OR (
          household_demographics.hd_dep_count = 4
          AND household_demographics.hd_vehicle_count <= 4 + 2
        )
        OR (
          household_demographics.hd_dep_count = 3
          AND household_demographics.hd_vehicle_count <= 3 + 2
        )
      )
      AND store.s_store_name = 'ese'
    GROUP BY page_id_0
  ) AS s1,
  (
    SELECT COUNT(*) AS h9_to_9_30,
      'page_id_1:' || CAST(
        (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_1
    FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
      household_demographics,
      time_dim,
      store
    WHERE ss_sold_time_sk = time_dim.t_time_sk
      AND ss_hdemo_sk = household_demographics.hd_demo_sk
      AND ss_store_sk = s_store_sk
      AND time_dim.t_hour = 9
      AND time_dim.t_minute < 30
      AND (
        (
          household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2
        )
        OR (
          household_demographics.hd_dep_count = 4
          AND household_demographics.hd_vehicle_count <= 4 + 2
        )
        OR (
          household_demographics.hd_dep_count = 3
          AND household_demographics.hd_vehicle_count <= 3 + 2
        )
      )
      AND store.s_store_name = 'ese'
    GROUP BY page_id_1
  ) AS s2,
  (
    SELECT COUNT(*) AS h9_30_to_10,
      'page_id_2:' || CAST(
        (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_2
    FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
      household_demographics,
      time_dim,
      store
    WHERE ss_sold_time_sk = time_dim.t_time_sk
      AND ss_hdemo_sk = household_demographics.hd_demo_sk
      AND ss_store_sk = s_store_sk
      AND time_dim.t_hour = 9
      AND time_dim.t_minute >= 30
      AND (
        (
          household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2
        )
        OR (
          household_demographics.hd_dep_count = 4
          AND household_demographics.hd_vehicle_count <= 4 + 2
        )
        OR (
          household_demographics.hd_dep_count = 3
          AND household_demographics.hd_vehicle_count <= 3 + 2
        )
      )
      AND store.s_store_name = 'ese'
    GROUP BY page_id_2
  ) AS s3,
  (
    SELECT COUNT(*) AS h10_to_10_30,
      'page_id_3:' || CAST(
        (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_3
    FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
      household_demographics,
      time_dim,
      store
    WHERE ss_sold_time_sk = time_dim.t_time_sk
      AND ss_hdemo_sk = household_demographics.hd_demo_sk
      AND ss_store_sk = s_store_sk
      AND time_dim.t_hour = 10
      AND time_dim.t_minute < 30
      AND (
        (
          household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2
        )
        OR (
          household_demographics.hd_dep_count = 4
          AND household_demographics.hd_vehicle_count <= 4 + 2
        )
        OR (
          household_demographics.hd_dep_count = 3
          AND household_demographics.hd_vehicle_count <= 3 + 2
        )
      )
      AND store.s_store_name = 'ese'
    GROUP BY page_id_3
  ) AS s4,
  (
    SELECT COUNT(*) AS h10_30_to_11,
      'page_id_4:' || CAST(
        (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_4
    FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
      household_demographics,
      time_dim,
      store
    WHERE ss_sold_time_sk = time_dim.t_time_sk
      AND ss_hdemo_sk = household_demographics.hd_demo_sk
      AND ss_store_sk = s_store_sk
      AND time_dim.t_hour = 10
      AND time_dim.t_minute >= 30
      AND (
        (
          household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2
        )
        OR (
          household_demographics.hd_dep_count = 4
          AND household_demographics.hd_vehicle_count <= 4 + 2
        )
        OR (
          household_demographics.hd_dep_count = 3
          AND household_demographics.hd_vehicle_count <= 3 + 2
        )
      )
      AND store.s_store_name = 'ese'
    GROUP BY page_id_4
  ) AS s5,
  (
    SELECT COUNT(*) AS h11_to_11_30,
      'page_id_5:' || CAST(
        (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_5
    FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
      household_demographics,
      time_dim,
      store
    WHERE ss_sold_time_sk = time_dim.t_time_sk
      AND ss_hdemo_sk = household_demographics.hd_demo_sk
      AND ss_store_sk = s_store_sk
      AND time_dim.t_hour = 11
      AND time_dim.t_minute < 30
      AND (
        (
          household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2
        )
        OR (
          household_demographics.hd_dep_count = 4
          AND household_demographics.hd_vehicle_count <= 4 + 2
        )
        OR (
          household_demographics.hd_dep_count = 3
          AND household_demographics.hd_vehicle_count <= 3 + 2
        )
      )
      AND store.s_store_name = 'ese'
    GROUP BY page_id_5
  ) AS s6,
  (
    SELECT COUNT(*) AS h11_30_to_12,
      'page_id_6:' || CAST(
        (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_6
    FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
      household_demographics,
      time_dim,
      store
    WHERE ss_sold_time_sk = time_dim.t_time_sk
      AND ss_hdemo_sk = household_demographics.hd_demo_sk
      AND ss_store_sk = s_store_sk
      AND time_dim.t_hour = 11
      AND time_dim.t_minute >= 30
      AND (
        (
          household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2
        )
        OR (
          household_demographics.hd_dep_count = 4
          AND household_demographics.hd_vehicle_count <= 4 + 2
        )
        OR (
          household_demographics.hd_dep_count = 3
          AND household_demographics.hd_vehicle_count <= 3 + 2
        )
      )
      AND store.s_store_name = 'ese'
    GROUP BY page_id_6
  ) AS s7,
  (
    SELECT COUNT(*) AS h12_to_12_30,
      'page_id_7:' || CAST(
        (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_7
    FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
      household_demographics,
      time_dim,
      store
    WHERE ss_sold_time_sk = time_dim.t_time_sk
      AND ss_hdemo_sk = household_demographics.hd_demo_sk
      AND ss_store_sk = s_store_sk
      AND time_dim.t_hour = 12
      AND time_dim.t_minute < 30
      AND (
        (
          household_demographics.hd_dep_count = -1
          AND household_demographics.hd_vehicle_count <= -1 + 2
        )
        OR (
          household_demographics.hd_dep_count = 4
          AND household_demographics.hd_vehicle_count <= 4 + 2
        )
        OR (
          household_demographics.hd_dep_count = 3
          AND household_demographics.hd_vehicle_count <= 3 + 2
        )
      )
      AND store.s_store_name = 'ese'
    GROUP BY page_id_7
  ) AS s8