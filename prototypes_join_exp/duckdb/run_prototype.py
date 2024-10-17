from typing import List
import duckdb
import time
from scipy.stats import norm
import os
import json
import argparse
import pandas as pd

# hyper parameters
pilot_sample_rate = 0.05 / 100
search_space = [0.0001 * i for i in range(1,10)] + [0.001 * i for i in range(1,10)] + [0.01 * i for i in range(1,10)] + [0.1 * i for i in range(1,10)]

def cold_start():
    os.system("sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches;")

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
    # var = c_empty / (a**2) * y_empty_est + c_l / (a**2) * y_l_est + c_o / (a**2) * y_o_est + c_lo / (a**2) * y_lo_est - y_empty_est
    var = c_l / (a**2) * y_l_est + c_o / (a**2) * y_o_est + c_lo / (a**2) * y_lo_est
    return var

def run_query(query_id: int, pilot_query: str, single_table_sample_query_t: str, 
              multi_table_sample_query_t: str, sampling_tables: List[str],
              debug: bool, mode: str = "single_table"):
    running_info = {}
    running_info["query_id"] = query_id

    if not debug:
        cold_start()

    if not debug and mode == "exact":
        conn = duckdb.connect("/mydata/tpch_1t.duckdb", read_only = True)
        query_file = f"query_{query_id}.sql"
        with open(query_file, "r") as f:
            query = f.read()
        print(f"running exact query {query}")
        start = time.time()
        exact_result = conn.sql(query).df()
        runtime = time.time() - start
        running_info["exact_time"] = runtime
        running_info["exact_result"] = exact_result.to_dict()
        with open("all_results.jsonl", "a+") as f:
            f.write(json.dumps(running_info) + "\n")
        conn.close()
        return

    # step-3: find the optimal sample rate for the case of multi-table sampling
    conn, sum_keys, y_empty_est, y_l_est, y_o_est, y_lo_est, max_var = run_pilot_query(pilot_query, sampling_tables, running_info)
    min_theta1 = 1
    min_theta2 = 1
    if sampling_tables[1] == "o":
        utility_fn = lambda theta1, theta2: 4 * theta1 + theta2
    else:
        utility_fn = lambda theta1, theta2: 30 * theta1 + theta2
    for theta1 in search_space:
        for theta2 in search_space:
            var = get_var(theta1, theta2, y_empty_est, y_l_est, y_o_est, y_lo_est)
            valid = True
            for sum_key in sum_keys:
                if (var[sum_key] > max_var[sum_key]).any():
                    valid = False
                    break
            if valid:
                print(f"theta1: {theta1}, theta2: {theta2}")
                if utility_fn(theta1, theta2) < utility_fn(min_theta1, min_theta2):
                    min_theta1 = theta1
                    min_theta2 = theta2
    running_info["multi_table_sample_rate_1"] = min_theta1 * 100
    running_info["multi_table_sample_rate_2"] = min_theta2 * 100
    print(f"min_theta1: {min_theta1}, min_theta2: {min_theta2}")
    if (min_theta1 <= 0.5 or min_theta2 <= 0.5) and not debug and mode == "multi_table":
        sample_query = multi_table_sample_query_t.format(sample_rate_1=f"{min_theta1*100:.2f}", sample_rate_2=f"{min_theta2*100:.2f}")
        print(f"running multi table sampling query {sample_query}")
        start = time.time()
        sample_result = conn.sql(sample_query).df()
        runtime = time.time() - start
        running_info["multi_table_sample_time"] = runtime
        running_info["multi_table_sample_result"] = sample_result.to_dict()
    else:
        running_info["multi_table_sample_time"] = -1
        running_info["multi_table_sample_result"] = {}

    # step-4: find the optimal sample rate for the case of single table sampling
    min_theta1 = 1
    for theta1 in search_space:
        var = get_var(theta1, 1, y_empty_est, y_l_est, y_o_est, y_lo_est)
        valid = True
        for sum_key in sum_keys:
            if (var[sum_key] > max_var[sum_key]).any():
                valid = False
                break
        if valid:
            min_theta1 = theta1
            break
    running_info["single_table_sample_rate"] = min_theta1 * 100
    print(f"min_theta1: {min_theta1}")
    if min_theta1 <= 0.1 and not debug and mode == "single_table":
        sample_query = single_table_sample_query_t.format(sample_rate_1=f"{min_theta1*100:.2f}")
        print(f"running single table sampling query {sample_query}")
        start = time.time()
        sample_result = conn.sql(sample_query).df()
        runtime = time.time() - start
        running_info["single_table_sample_time"] = runtime
        running_info["single_table_sample_result"] = sample_result.to_dict()
    else:
        running_info["single_table_sample_time"] = -1
        running_info["single_table_sample_result"] = {}

    with open("all_results.jsonl", "a+") as f:
        f.write(json.dumps(running_info) + "\n")
    conn.close()

def run_pilot_query(pilot_query, sampling_tables, running_info):
    conn = duckdb.connect("/mydata/tpch_1t.duckdb", read_only = True)
    # step-1: run pilot query
    start = time.time()
    print(f"running pilot query {pilot_query}")
    pilot_result = conn.execute(pilot_query).fetchdf()
    runtime = time.time() - start
    running_info["pilot_time"] = runtime

    # step-2: calculate the sum and variance of the pilot query
    pilot_result_columns = pilot_result.columns
    group_keys = [col for col in pilot_result_columns if not col.endswith("_pageid") and not col.startswith("sum_")]
    sum_keys = [col for col in pilot_result_columns if col.startswith("sum_")]
    pageid_keys = [sampling_table + "_pageid" for sampling_table in sampling_tables]
    if group_keys == []:
        y_empty_est = (pilot_result[sum_keys].sum() ** 2) / pilot_sample_rate / pilot_sample_rate
        print("y_empty_est", y_empty_est)
        y_l_est = (pilot_result.groupby([pageid_keys[0]])[sum_keys].sum() ** 2).sum() / pilot_sample_rate
        print("y_l_est", y_l_est)
        y_o_est = (pilot_result.groupby([pageid_keys[1]])[sum_keys].sum() ** 2).sum() / pilot_sample_rate / pilot_sample_rate
        print("biased y_o_est", y_o_est)
        y_lo_est = (pilot_result.groupby(pageid_keys)[sum_keys].sum() ** 2).sum() / pilot_sample_rate
        print("y_lo_est", y_lo_est)
        y_o_est -= y_lo_est * (1-pilot_sample_rate) / pilot_sample_rate
        print("unbiased y_o_est", y_o_est)
        sum_est = pilot_result[sum_keys].sum() / pilot_sample_rate
    else:
        y_empty_est = (pilot_result.groupby(group_keys)[sum_keys].sum() ** 2) / pilot_sample_rate / pilot_sample_rate
        print("y_empty_est", y_empty_est)
        y_l_est = (pilot_result.groupby(group_keys + [pageid_keys[0]])[sum_keys].sum() ** 2).groupby(group_keys).sum() / pilot_sample_rate
        print("y_l_est", y_l_est)
        y_o_est = (pilot_result.groupby(group_keys + [pageid_keys[1]])[sum_keys].sum() ** 2).groupby(group_keys).sum() / pilot_sample_rate / pilot_sample_rate
        print("biased y_o_est", y_o_est)
        y_lo_est = (pilot_result.groupby(group_keys + pageid_keys)[sum_keys].sum() ** 2).groupby(group_keys).sum() / pilot_sample_rate
        print("y_lo_est", y_lo_est)
        y_o_est -= y_lo_est * (1-pilot_sample_rate) / pilot_sample_rate
        print("unbiased y_o_est", y_o_est)
        sum_est = pilot_result.groupby(group_keys)[sum_keys].sum() / pilot_sample_rate
    sum_var = get_var(pilot_sample_rate, 1, y_empty_est, y_l_est, y_o_est, y_lo_est)
    z_val = norm.ppf(0.99)
    est_lb = sum_est - z_val * (sum_var ** 0.5)
    max_var = (0.05 * est_lb / z_val) ** 2
    print("sum_var", sum_var)
    print("sum_est", sum_est)
    print("sum_est_lb", est_lb)
    print("max_var", max_var)
    return conn,sum_keys,y_empty_est,y_l_est,y_o_est,y_lo_est,max_var

from query_templates import *

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query_id", type=int, required=True)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--mode", type=str, default="single_table")
    args = parser.parse_args()

    if args.query_id == 5:
        run_query(args.query_id, pilot_query_5, single_table_sample_query_5, multi_table_sample_query_5, ["l", "o"], args.debug, args.mode)
    elif args.query_id == 7:
        run_query(args.query_id, pilot_query_7, single_table_sample_query_7, multi_table_sample_query_7, ["l", "o"], args.debug, args.mode)
    elif args.query_id == 8:
        run_query(args.query_id, pilot_query_8, single_table_sample_query_8, multi_table_sample_query_8, ["l", "o"], args.debug, args.mode)
    elif args.query_id == 9:
        run_query(args.query_id, pilot_query_9, single_table_sample_query_9, multi_table_sample_query_9, ["l", "o"], args.debug, args.mode)
    elif args.query_id == 12:
        run_query(args.query_id, pilot_query_12, single_table_sample_query_12, multi_table_sample_query_12, ["l", "o"], args.debug, args.mode)
    elif args.query_id == 14:
        run_query(args.query_id, pilot_query_14, single_table_sample_query_14, multi_table_sample_query_14, ["l", "p"], args.debug, args.mode)
    elif args.query_id == 19:
        run_query(args.query_id, pilot_query_19, single_table_sample_query_19, multi_table_sample_query_19, ["l", "p"], args.debug, args.mode)