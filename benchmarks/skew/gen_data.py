# lineitem: {l_quantity, l_orderkey}

# l_quantity in [1, 50]     zipf with unit 1
# l_orderkey from orderkey  zipf with unit 1

# orders: {o_orderkey, o_orderdate}
# o_orderkey = [1, ..., 10 * 150000 * scale]
# o_orderdate in [92001, 92001+2557-(121+30)-1] zipf with unit 1

# query 1
# SELECT SUM(l_quantity) FROM lineitem

# query 2
# SELECT SUM(l_quantity) FROM lineitem, orders WHERE l_orderkey = o_orderkey

# query 3
# SELECT SUM(l_quantity), EXTRACT(YEAR FROM o_orderdate) o_year
# From lineitem, orders WHERE l_orderkey = o_orderkey GROUP BY o_year

import numpy as np
import pandas as pd
import argparse
import os
import time
import csv
from tqdm import tqdm

def int_to_date(dates_int: np.ndarray):
    # start date: 1990-01-01
    start_date = np.datetime64("1990-01-01")
    return dates_int + start_date

def gen_data_zipf(min_int: int, max_int: int, n_data: int, param: float):
    from scipy.stats import zipfian
    n_elements = max_int - min_int + 1
    data = np.array(zipfian.rvs(param, n_elements, size=n_data))
    data += min_int
    return data

def gen_order(scale: int, skew_param: float, save_dir: str, n_parallel: int=1, child: int=1):
    n_data_per_scale = 1500000
    n_total_dates = 2405
    n_data = scale * n_data_per_scale
    n_data_per_child = n_data // n_parallel
    start_idx = n_data_per_child * (child - 1)
    if child == n_parallel:
        n_data_gen = n_data - n_data_per_child * (n_parallel - 1)
    else:
        n_data_gen = n_data_per_child

    part_len = 1000
    save_path = f"{save_dir}/order_{child}.csv"

    for part_id in tqdm(range(n_data_gen // part_len + 1)):
        if part_id == n_data_gen // part_len:
            n_data_part = n_data_gen % part_len if n_data_gen % part_len != 0 else part_len
        else:
            n_data_part = part_len
        o_orderkey = np.array(list(range(start_idx, start_idx + n_data_part)))
        o_orderdate = gen_data_zipf(0, n_total_dates, n_data_part, skew_param)
        o_orderdate = int_to_date(o_orderdate)
        with open(save_path, "a+") as f:
            writer = csv.writer(f)
            data = list(map(list, zip(o_orderkey, o_orderdate)))
            writer.writerows(data)

def gen_lineitem(scale: int, skew_param: float, save_dir: str, n_parallel: int=1, child: int=1):
    n_data_per_scale = 6000000
    n_data = n_data_per_scale * scale
    n_data_per_child = n_data // n_parallel

    if child == n_parallel:
        n_data_gen = n_data - n_data_per_child * (n_parallel - 1)
    else:
        n_data_gen = n_data_per_child

    part_len = 1000
    save_path = f"{save_dir}/lineitem_{child}.csv"

    for part_id in tqdm(range(n_data_gen // part_len + 1)):
        if part_id == n_data_gen // part_len:
            n_data_part = n_data_gen % part_len if n_data_gen % part_len != 0 else part_len
        else:
            n_data_part = part_len
        l_quantity = gen_data_zipf(1, 50, n_data_part, skew_param)
        l_orderkey = gen_data_zipf(1, 10 * 150000 * scale, n_data_part, skew_param)
        with open(save_path, "a+") as f:
            writer = csv.writer(f)
            data = list(map(list, zip(l_quantity, l_orderkey)))
            writer.writerows(data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, default=100, help="scale factor of the data")
    parser.add_argument("--skew", type=float, default=1.5, help="parameter of the zipfian distribution")
    parser.add_argument("--save_dir", type=str, default="/mydata/skew", help="where to save data")
    parser.add_argument("--seed", type=int, default=2333, help="random seed")
    parser.add_argument("--parallel", type=int, default=1, help="number of parallel generations")
    parser.add_argument("--child", type=int, default=1, help="the i-th child of generation")
    parser.add_argument("--table", type=str, default="order", help="specific table to generate")
    args = parser.parse_args()

    # initialize
    np.random.seed(args.seed)
    if not os.path.exists(args.save_dir):
        os.makedirs(args.save_dir, exist_ok=True)
    
    # start generating
    print("generating synthetic skewed data distribution based on TPC-H")
    print(f"with scale {args.scale}, zipfian parameter {args.skew}, and seed {args.seed}")
    print(f"saving data to {args.save_dir}")

    if args.table == "order":
        gen_order(args.scale, args.skew, args.save_dir, args.parallel, args.child)
    elif args.table == "lineitem":
        gen_lineitem(args.scale, args.skew, args.save_dir, args.parallel, args.child)
