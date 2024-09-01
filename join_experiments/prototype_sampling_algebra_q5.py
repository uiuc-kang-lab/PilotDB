import pandas.io.sql as sqlio
import pandas as pd
import psycopg2
import time
import warnings
import numpy as np
from scipy.stats import norm
import os
import json


warnings.simplefilter(action='ignore', category=UserWarning)

pilot_query = """
select
    n_name,
    (lineitem.ctid::text::point)[0] as l_page_id,
    (orders.ctid::text::point)[0] as o_page_id,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    customer,
    orders,
    lineitem TABLESAMPLE SYSTEM (0.05),
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
    l_page_id,
    o_page_id;
"""

sample_query_t = """
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) * 10000 / {sample_rate_o} / {sample_rate_l} as revenue
from
    customer,
    orders TABLESAMPLE SYSTEM ({sample_rate_o}),
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
    n_name;
"""

sample_query_old = """
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) * 100 / {sample_rate_l} as revenue
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
    n_name;
"""

def get_var(theta1, theta2, y_empty_est, y_l_est, y_o_est, y_lo_est):
    a = theta1 * theta2
    b_empty = (theta1 ** 2) * (theta2 ** 2)
    b_l = theta1 * (theta2 **2)
    b_o = (theta1 ** 2) * theta2
    b_lo = theta1 * theta2
    c_empty = b_empty
    c_l = -b_empty + b_l
    c_o = -b_empty + b_o
    c_lo = b_empty - b_l - b_o + b_lo
    var = c_empty / (a**2) * y_empty_est + c_l / (a**2) * y_l_est + c_o / (a**2) * y_o_est + c_lo / (a**2) * y_lo_est - y_empty_est
    return var

os.system("pg_ctl -D /mydata/tpch/tpch/ps stop; sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches; pg_ctl -D /mydata/tpch/tpch/ps start")
all_result = {}
with psycopg2.connect(dbname="tpch1t", user="teng77", host="localhost", port=5432) as conn:
    start = time.time()
 
    pilot_result = sqlio.read_sql_query(pilot_query, conn)
    pilot_result = pilot_result[pilot_result["n_name"] == "CHINA                    "]
    print("time to process pilot query", time.time() - start)
    runtime = time.time() - start
    all_result["pilot_time"] = runtime

    pilot_sample_rate = 0.0005

    sum_est = pilot_result["revenue"].sum() / pilot_sample_rate 

    y_empty_est = (pilot_result["revenue"].sum() ** 2).item() / pilot_sample_rate / pilot_sample_rate 
    y_l_est = np.sum(pilot_result.groupby("l_page_id").sum()["revenue"] ** 2).item() / pilot_sample_rate 
    y_o_est = np.sum(pilot_result.groupby("o_page_id").sum()["revenue"] ** 2).item() / pilot_sample_rate / pilot_sample_rate 
    y_lo_est = np.sum(pilot_result["revenue"] ** 2).item() / pilot_sample_rate 

    sum_var = get_var(pilot_sample_rate, 1, y_empty_est, y_l_est, y_o_est, y_lo_est)

    print("sum_est", sum_est)
    print("sum_var", sum_var)

    z_val = norm.ppf(0.99)

    est_lb = sum_est - z_val * (sum_var ** 0.5)

    max_var = (0.05 * est_lb / z_val) ** 2

    ### old method
    min_theta1 = 1
    for theta1 in [0.001 * i for i in range(1,1000)]:
        var = get_var(theta1, 1, y_empty_est, y_l_est, y_o_est, y_lo_est)
        if var < max_var:
            min_theta1 = theta1
            break
    
    sample_query = sample_query_old.format(sample_rate_l=f"{min_theta1*100:.2f}")
    print(sample_query)

    start = time.time()
    sample_result = sqlio.read_sql_query(sample_query, conn)
    runtime = time.time() - start
    print("time to process sample query single table:", runtime)
    all_result["single_table_rate"] = min_theta1*100
    all_result["single_table_time"] = runtime
    result = sample_result["revenue"].to_dict()
    all_result["single_table_result"] = result

    ### new method
    min_theta1 = 1
    min_theta2 = 1
    utility = 4*min_theta1 # + min_theta2
    for theta1 in [0.001 * i for i in range(1,501)]:
        for theta2 in [0.001 * i for i in range(1,501)]:
            var = get_var(theta1, theta2, y_empty_est, y_l_est, y_o_est, y_lo_est)
            if var < max_var:
                current_ut = 4*theta1 + theta2
                if current_ut < utility:
                    utility = current_ut
                    min_theta1 = theta1
                    min_theta2 = theta2
    if min_theta1 < 1 and min_theta2 < 1:
        sample_query = sample_query_t.format(sample_rate_l=f"{min_theta1*100:.2f}", sample_rate_o=f"{min_theta2*100:.2f}")
        print(sample_query)
        all_result["multi_table_rate_l"] = min_theta1*100
        all_result["multi_table_rate_o"] = min_theta2*100

        start = time.time()
        sample_result = sqlio.read_sql_query(sample_query, conn)
        runtime = time.time() - start
        print("time to process sample query multi table:", runtime)
        all_result["multi_table_time"] = runtime
        result = sample_result["revenue"].to_dict()
        all_result["multi_table_result"] = result

with open("all_results_q5.jsonl", "a+") as f:
    f.write(json.dumps(all_result) + "\n")
    