\timing
-- COPY (
select
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    (orders.ctid::text::point)[0] o_page_id,
    (lineitem.ctid::text::point)[0] l_page_id
from
    orders,
    lineitem TABLESAMPLE SYSTEM (0.05)
where
    l_orderkey = o_orderkey
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1994-01-01' + interval '1' year
group by
    o_page_id,
    l_page_id
--) TO '/users/yuxuan18/PilotDB/join_experiments/q5_simple_pilot_result_l.csv' DELIMITER ',' CSV HEADER
;

-- COPY (
select
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    (orders.ctid::text::point)[0] o_page_id,
    (lineitem.ctid::text::point)[0] l_page_id
from
    orders TABLESAMPLE SYSTEM (0.01),
    lineitem
where
    l_orderkey = o_orderkey
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1994-01-01' + interval '1' year
group by
    o_page_id,
    l_page_id
-- ) TO '/users/yuxuan18/PilotDB/join_experiments/q5_simple_pilot_result_o.csv' DELIMITER ',' CSV HEADER
;
