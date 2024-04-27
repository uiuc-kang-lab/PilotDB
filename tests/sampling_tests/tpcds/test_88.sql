SELECT * FROM (SELECT COUNT(*) / sample_rate AS h8_30_to_9 FROM store_sales sampling_method, household_demographics, time_dim, store WHERE ss_sold_time_sk = time_dim.t_time_sk AND ss_hdemo_sk = household_demographics.hd_demo_sk AND ss_store_sk = s_store_sk AND time_dim.t_hour = 8 AND time_dim.t_minute >= 30 AND ((household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2) OR (household_demographics.hd_dep_count = 4 AND household_demographics.hd_vehicle_count <= 4 + 2) OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)) AND store.s_store_name = 'ese') AS s1, (SELECT COUNT(*) / sample_rate AS h9_to_9_30 FROM store_sales sampling_method, household_demographics, time_dim, store WHERE ss_sold_time_sk = time_dim.t_time_sk AND ss_hdemo_sk = household_demographics.hd_demo_sk AND ss_store_sk = s_store_sk AND time_dim.t_hour = 9 AND time_dim.t_minute < 30 AND ((household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2) OR (household_demographics.hd_dep_count = 4 AND household_demographics.hd_vehicle_count <= 4 + 2) OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)) AND store.s_store_name = 'ese') AS s2, (SELECT COUNT(*) / sample_rate AS h9_30_to_10 FROM store_sales sampling_method, household_demographics, time_dim, store WHERE ss_sold_time_sk = time_dim.t_time_sk AND ss_hdemo_sk = household_demographics.hd_demo_sk AND ss_store_sk = s_store_sk AND time_dim.t_hour = 9 AND time_dim.t_minute >= 30 AND ((household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2) OR (household_demographics.hd_dep_count = 4 AND household_demographics.hd_vehicle_count <= 4 + 2) OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)) AND store.s_store_name = 'ese') AS s3, (SELECT COUNT(*) / sample_rate AS h10_to_10_30 FROM store_sales sampling_method, household_demographics, time_dim, store WHERE ss_sold_time_sk = time_dim.t_time_sk AND ss_hdemo_sk = household_demographics.hd_demo_sk AND ss_store_sk = s_store_sk AND time_dim.t_hour = 10 AND time_dim.t_minute < 30 AND ((household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2) OR (household_demographics.hd_dep_count = 4 AND household_demographics.hd_vehicle_count <= 4 + 2) OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)) AND store.s_store_name = 'ese') AS s4, (SELECT COUNT(*) / sample_rate AS h10_30_to_11 FROM store_sales sampling_method, household_demographics, time_dim, store WHERE ss_sold_time_sk = time_dim.t_time_sk AND ss_hdemo_sk = household_demographics.hd_demo_sk AND ss_store_sk = s_store_sk AND time_dim.t_hour = 10 AND time_dim.t_minute >= 30 AND ((household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2) OR (household_demographics.hd_dep_count = 4 AND household_demographics.hd_vehicle_count <= 4 + 2) OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)) AND store.s_store_name = 'ese') AS s5, (SELECT COUNT(*) / sample_rate AS h11_to_11_30 FROM store_sales sampling_method, household_demographics, time_dim, store WHERE ss_sold_time_sk = time_dim.t_time_sk AND ss_hdemo_sk = household_demographics.hd_demo_sk AND ss_store_sk = s_store_sk AND time_dim.t_hour = 11 AND time_dim.t_minute < 30 AND ((household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2) OR (household_demographics.hd_dep_count = 4 AND household_demographics.hd_vehicle_count <= 4 + 2) OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)) AND store.s_store_name = 'ese') AS s6, (SELECT COUNT(*) / sample_rate AS h11_30_to_12 FROM store_sales sampling_method, household_demographics, time_dim, store WHERE ss_sold_time_sk = time_dim.t_time_sk AND ss_hdemo_sk = household_demographics.hd_demo_sk AND ss_store_sk = s_store_sk AND time_dim.t_hour = 11 AND time_dim.t_minute >= 30 AND ((household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2) OR (household_demographics.hd_dep_count = 4 AND household_demographics.hd_vehicle_count <= 4 + 2) OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)) AND store.s_store_name = 'ese') AS s7, (SELECT COUNT(*) / sample_rate AS h12_to_12_30 FROM store_sales sampling_method, household_demographics, time_dim, store WHERE ss_sold_time_sk = time_dim.t_time_sk AND ss_hdemo_sk = household_demographics.hd_demo_sk AND ss_store_sk = s_store_sk AND time_dim.t_hour = 12 AND time_dim.t_minute < 30 AND ((household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2) OR (household_demographics.hd_dep_count = 4 AND household_demographics.hd_vehicle_count <= 4 + 2) OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)) AND store.s_store_name = 'ese') AS s8