import pandas as pd


def load_data(z: str, ub: float):
    data = pd.read_csv(f"/mydata/skew_data/{z}_postgres.csv")
    data = data[data["x"] <= ub ]
    return data

def get_mean_std(data: pd.DataFrame):
    grouping = data.groupby("x").agg({"x": ["size", "sum"]})
    page_sums = grouping["x"]["sum"]
    page_sizes = grouping["x"]["size"]
    page_sum_mean = page_sums.mean()
    page_sum_std = page_sums.std()
    page_size_mean = page_sizes.mean()
    page_size_std = page_sizes.std()
    x_std = data["x"].std()
    x_mean = data["x"].mean()

    return page_sum_mean, page_sum_std, page_size_mean, page_size_std, x_mean, x_std

page_stats = {}
for z in ["1_5", "2", "2_5", "3", "3_5", "4"]:
    data = load_data(z, ub=100)
    page_sum_mean, page_sum_std, page_size_mean, page_size_std, x_mean, x_std = get_mean_std(data)
    print(f"z={z}, sum_cov={page_sum_std/page_sum_mean:.2f}, cnt_cov={page_size_std/page_size_mean:.2f}, x_cov={x_std/x_mean:.2f}")
    page_stats[z] = {
        "sum": page_sum_std/page_sum_mean,
        "cnt": page_size_std/page_size_mean
    }

import json

with open("page_stats.json", "w") as f:
    json.dump(page_stats, f, indent=4)