import pandas.io.sql as sqlio
import pandas as pd
import psycopg2
import time
import warnings
import numpy as np
from scipy.stats import t, norm, chi2
import math


warnings.simplefilter(action='ignore', category=UserWarning)
pilot_query_1 = """
select
    n_name,
    orders_part.page_id as o_pageid,
    lineitem_part.page_id as l_pageid,
    revenue
from (
    select 
        o_orderkey,
        c_nationkey,
        (orders.ctid::text::point)[0] as page_id
    from
        customer,
        orders
    where
        c_custkey = o_custkey
        and o_orderdate >= date '1994-01-01'
        and o_orderdate < date '1994-01-01' + interval '1' year
    ) as orders_part
FULL JOIN
    (
    select 
        l_extendedprice * (1 - l_discount) as revenue,
        n_name,
        l_orderkey,
        s_nationkey,
        (lineitem.ctid::text::point)[0] as page_id
    from
        lineitem TABLESAMPLE SYSTEM ({sample_rate}),
        supplier,
        nation,
        region
    where
        l_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
    ) as lineitem_part
ON
    l_orderkey = o_orderkey
    and c_nationkey = s_nationkey
"""

pilot_query_2 = """
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    (orders.ctid::text::point)[0] as o_pageid,
    (lineitem.ctid::text::point)[0] as l_pageid
from
    customer,
    orders,
    lineitem TABLESAMPLE SYSTEM ({sample_rate_l}),
    supplier,
    nation,
    region
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'ASIA'
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1994-01-01' + interval '1' year
group by
    n_name,
    o_pageid,
    l_pageid;
"""

def _solve_quadratic(a, b, c):
    return (-b - math.sqrt(b**2 - 4*a*c)) / (2*a), (-b + math.sqrt(b**2 - 4*a*c)) / (2*a)

def get_mean_lb(sample_size: int, sample_mean: float, sample_std: float, failure_probability: float):
    t_val = t.ppf(1-failure_probability, sample_size-1)
    return sample_mean - t_val * sample_std / (sample_size ** 0.5)

def get_std_ub(sample_size: int, sample_std: float, failure_probability: float):
    chi2_val = chi2.ppf(failure_probability, sample_size-1)
    return sample_std * ((sample_size-1) / chi2_val) ** 0.5

def get_bernoulli_N_lb(sample_size: int, sample_rate: float, failure_probability: float):
    z_val = norm.ppf(1-failure_probability)
    return _solve_quadratic(a=sample_rate**2,
                            b=-2*sample_rate*sample_size - z_val*sample_rate*(1-sample_rate),
                            c=sample_size**2)[0]

def get_sample_rate(fp: float, sample_size: int, pilot_sample_rate: float, pilot_sample_size: int):
    if pilot_sample_rate > 0.9:
        bernoulli_N_lb = pilot_sample_size
    else:
        bernoulli_N_lb = get_bernoulli_N_lb(pilot_sample_size, pilot_sample_rate, fp)
    assert sample_size <= bernoulli_N_lb, f"{sample_size} is too big"
    z_val = norm.ppf(1-fp)
    p = _solve_quadratic(a=bernoulli_N_lb**2 + z_val**2 * bernoulli_N_lb,
                        b=-(2*bernoulli_N_lb*sample_size + z_val**2 * bernoulli_N_lb),
                        c=sample_size**2)[1]
    return p

with psycopg2.connect(dbname="tpch1t", user="teng77", host="localhost", port=5432) as conn:
    # start = time.time()
    # pilot_result_1 = sqlio.read_sql_query(pilot_query_1.format(sample_rate=0.05), conn)
    # print("time to process pilot query 1", time.time() - start)
    start = time.time()
    pilot_result_2 = sqlio.read_sql_query(pilot_query_2.format(sample_rate_l=0.05), conn)
    print("time to process pilot query 2", time.time() - start)

    # pilot_result_1.to_csv("q5_pilot_result_1.csv", index=False)
    pilot_result_2.to_csv("q5_pilot_result_2.csv", index=False)

    # pilot_result_1 = pd.read_csv("q5_pilot_result_1.csv")
    pilot_result_2 = pd.read_csv("q5_pilot_result_1.csv")

    exit()
    group_keys = pilot_result_1["n_name"].unique()
    n_pages_l = pilot_result_1.groupby("n_name")["l_pageid"].nunique()
    # n_pages_o = pilot_result_2["page_id"].nunique()
    print("group keys", group_keys)
    print("n_pages_l\n", n_pages_l)
    # print("n_pages_o\n", n_pages_o)

    join_result = pd.merge(pilot_result_1, pilot_result_2, 
                           how="inner", left_on=["l_orderkey", "s_nationkey"], 
                           right_on=["o_orderkey", "c_nationkey"], suffixes=('_l', '_o'))
    

    for group_key in group_keys:
        group_join_result = join_result[join_result["n_name"] == group_key]
        sum_l = np.array(group_join_result.groupby("page_id_l").agg({"revenue": "sum"})["revenue"] / n_pages_o)
        sum_o = np.array(group_join_result.groupby("page_id_o").agg({"revenue": "sum"})["revenue"] / n_pages_l[group_key])
        print(len(sum_l)/n_pages_l[group_key], len(sum_o)/n_pages_o)
        sum_l = np.concatenate([sum_l, np.zeros(n_pages_l[group_key]  - len(sum_l))])
        sum_o = np.concatenate([sum_o, np.zeros(n_pages_o - len(sum_o))])

        var_l = np.var(sum_l).item()
        var_o = np.var(sum_o).item()
        std = (var_l + var_o / (n_pages_o / n_pages_l[group_key])) ** 0.5
        avg = group_join_result["revenue"].sum() / n_pages_o / n_pages_l[group_key]
        print("var_l", var_l)
        print("var_o", var_o)
        print("std", std)
        print("avg", avg)

        avg_lb = get_mean_lb(n_pages_l[group_key], avg, std, 0.001)
        print("avg_lb", avg_lb)

        var_l_ub = get_std_ub(n_pages_l[group_key], var_l ** 0.5, 0.001) ** 2
        var_o_ub = get_std_ub(n_pages_o, var_o ** 0.5, 0.001) ** 2
        print("var_l_ub", var_l_ub)
        print("var_o_ub", var_o_ub)

        z_val = norm.ppf(0.999)
        sample_size = (z_val ** 2) * (var_l + var_o) / (avg ** 2) / (0.05 ** 2)
        print("sample_size", sample_size)
        
        sample_rate_l = get_sample_rate(0.001, sample_size, 0.5/100, n_pages_l[group_key])
        print("sample_rate_l", sample_rate_l)
        sample_rate_o = get_sample_rate(0.001, sample_size, 0.5/100, n_pages_o)
        print("sample_rate_o", sample_rate_o)

        print("\n\n")








        