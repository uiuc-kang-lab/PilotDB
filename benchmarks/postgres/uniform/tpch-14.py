pilot_query = '''
select
    avg(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) as avg_1,
    stdev(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) as std_1,
    avg(l_extendedprice * (1 - l_discount)) as avg_2,
    stdev(l_extendedprice * (1 - l_discount)) as std_2,
    count(*) as sample_size
from
    lineitem,
    part
where
    l_partkey = p_partkey
    and l_shipdate >= date '1995-09-01'
    and l_shipdate < date '1995-09-01' + interval '1' month;
'''

results_mapping = [
    {"aggregate": "div", "first_element": "avg_1", "second_element": "avg_2", "size": "sample_size"}
]

subquery_dict = []