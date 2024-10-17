pilot_query = """
select
    avg(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) as avg_1,
    stddev(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) as std_1,
    avg(l_extendedprice * (1 - l_discount)) as avg_2,
    stddev(l_extendedprice * (1 - l_discount)) as std_2,
    count(*) as sample_size
from
    lineitem {sampling_method},
    part
where
    l_partkey = p_partkey
    and l_shipdate >= date '1995-09-01'
    and l_shipdate < date '1995-09-01' + interval '1' month;
"""

sampling_query = """
SELECT 100.00 * SUM(
    CASE
      WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount)
      ELSE 0
    END
  ) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM lineitem,
  part
WHERE l_partkey = p_partkey
  AND l_shipdate >= '1995-09-01'
  AND l_shipdate < DATEADD(MONTH, 1, '1995-09-01')
  AND {sampling_method}
"""
results_mapping = [
    {
        "aggregate": "div",
        "first_element": "avg_1",
        "second_element": "avg_2",
        "size": "sample_size",
    }
]

subquery_dict = []
