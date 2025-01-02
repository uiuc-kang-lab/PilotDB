select
    sum(l_extendedprice * l_discount) as revenue
from (
    SELECT
        l_extendedprice,
        l_discount,
        l_shipdate,
        l_discount,
        l_quantity
    FROM
        lineitem
    ORDER BY RANDOM()
    LIMIT {sample_size}
)
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
    and l_quantity < 24;
