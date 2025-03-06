SELECT COUNT(ss_wholesale_cost),
count(distinct (store_sales.ctid::text::point)[0]),
count(distinct (store.ctid::text::point)[0])
FROM store_sales, store WHERE ss_store_sk = s_store_sk AND s_number_employees = 19095040


SELECT COUNT(*)
FROM store_sales, store WHERE ss_store_sk = s_store_sk AND s_number_employees = 19095040
GROUP BY (store_sales.ctid::text::point)[0]

SELECT COUNT(ss_wholesale_cost),
count(distinct (store_sales.ctid::text::point)[0]),
count(distinct (store.ctid::text::point)[0])
FROM store_sales, store WHERE ss_store_sk = s_store_sk
GROUP BY s_number_employees



select avg(ss_quantity)
,avg(ss_ext_sales_price)
,avg(ss_ext_wholesale_cost)
,sum(ss_ext_wholesale_cost)
from store_sales
   ,store
   ,customer_demographics
   ,household_demographics
   ,customer_address
   ,date_dim
where s_store_sk = ss_store_sk
and  ss_sold_date_sk = d_date_sk and d_year = 2001
and((ss_hdemo_sk=hd_demo_sk
and cd_demo_sk = ss_cdemo_sk
and cd_marital_status = 'S'
and cd_education_status = 'Unknown'
and ss_sales_price between 100.00 and 150.00
and hd_dep_count = 3
   )or
   (ss_hdemo_sk=hd_demo_sk
and cd_demo_sk = ss_cdemo_sk
and cd_marital_status = 'D'
and cd_education_status = 'Unknown'
and ss_sales_price between 50.00 and 100.00
and hd_dep_count = 1
   ) or
   (ss_hdemo_sk=hd_demo_sk
and cd_demo_sk = ss_cdemo_sk
and cd_marital_status = 'W'
and cd_education_status = 'Unknown'
and ss_sales_price between 150.00 and 200.00
and hd_dep_count = 1
   ))
and((ss_addr_sk = ca_address_sk
and ca_country = 'United States'
and ca_state in ('KS', 'LA', 'OK')
and ss_net_profit between 100 and 200
   ) or
   (ss_addr_sk = ca_address_sk
and ca_country = 'United States'
and ca_state in ('GA', 'ME', 'NC')
and ss_net_profit between 150 and 300
   ) or
   (ss_addr_sk = ca_address_sk
and ca_country = 'United States'
and ca_state in ('IA', 'IL', 'MI')
and ss_net_profit between 50 and 250
   ))
;


