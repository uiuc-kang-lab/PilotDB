\timing
--COPY 
--(
select
    sum(l_extendedprice * (1 - l_discount)) as revenue
   -- (orders.ctid::text::point)[0] o_page_id,
    --(lineitem.ctid::text::point)[0] l_page_id
from
    orders,
    lineitem
where
    l_orderkey = o_orderkey
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1994-01-01' + interval '1' year
-- group by
--    o_page_id,
--    l_page_id
-- ) TO '/mydata/q5_simple.csv' DELIMITER ',' CSV HEADER;
