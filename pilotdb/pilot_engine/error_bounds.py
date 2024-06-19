
from scipy.stats import t, chi2, norm
from typing import Dict, List
import math
import pandas as pd
import logging

def get_mean_ub(sample_size: int, sample_mean: float, sample_std: float, failure_probability: float):
    t_val = t.ppf(1-failure_probability, sample_size-1)
    return sample_mean + t_val * sample_std / (sample_size ** 0.5)

def get_mean_lb(sample_size: int, sample_mean: float, sample_std: float, failure_probability: float):
    t_val = t.ppf(1-failure_probability, sample_size-1)
    return sample_mean - t_val * sample_std / (sample_size ** 0.5)

def get_mean_two_side(sample_size: int, sample_mean: float, sample_std: float, failure_probability: float):
    failure_probability = failure_probability / 2
    lb = get_mean_lb(sample_size, sample_mean, sample_std, failure_probability)
    ub = get_mean_ub(sample_size, sample_mean, sample_std, failure_probability)
    return lb, ub

def get_std_lb(sample_size: int, sample_std: float, failure_probability: float):
    chi2_val = chi2.ppf(1-failure_probability, sample_size-1)
    return sample_std * ((sample_size-1) / chi2_val) ** 0.5

def get_std_ub(sample_size: int, sample_std: float, failure_probability: float):
    chi2_val = chi2.ppf(failure_probability, sample_size-1)
    return sample_std * ((sample_size-1) / chi2_val) ** 0.5

def get_std_two_side(sample_size: int, sample_std: float, failure_probability: float):
    failure_probability = failure_probability / 2
    lb = get_std_lb(sample_size, sample_std, failure_probability)
    ub = get_std_ub(sample_size, sample_std, failure_probability)
    return lb, ub

def get_bernoulli_N_ub(sample_size: int, sample_rate: float, failure_probability: float):
    z_val = norm.ppf(1-failure_probability)
    return _solve_quadratic(a=sample_rate**2,
                            b=-2*sample_rate*sample_size - z_val*sample_rate*(1-sample_rate),
                            c=sample_size**2)[1]

def get_bernoulli_N_lb(sample_size: int, sample_rate: float, failure_probability: float):
    z_val = norm.ppf(1-failure_probability)
    return _solve_quadratic(a=sample_rate**2,
                            b=-2*sample_rate*sample_size - z_val*sample_rate*(1-sample_rate),
                            c=sample_size**2)[0]

def get_bernoulli_N_two_side(sample_size: int, sample_rate: float, failure_probability: float):
    failure_probability = failure_probability / 2
    z_val = norm.ppf(1-failure_probability)
    return _solve_quadratic(a=sample_rate**2,
                            b=-2*sample_rate*sample_size - z_val*sample_rate*(1-sample_rate),
                            c=sample_size**2)

def get_mean_sample_size(error, fp: float, fp1: float, fp2: float, pilot_sample_mean, pilot_sample_std, pilot_sample_size):
    std_ub = get_std_ub(pilot_sample_size, pilot_sample_std, failure_probability=fp1)
    mean_lb = get_mean_lb(pilot_sample_size, pilot_sample_mean, pilot_sample_std, failure_probability=fp2)
    z_val = norm.ppf(1-fp/2)
    return (z_val/error * std_ub / mean_lb) ** 2

def _solve_quadratic(a, b, c):
    return (-b - math.sqrt(b**2 - 4*a*c)) / (2*a), (-b + math.sqrt(b**2 - 4*a*c)) / (2*a)

def get_sample_rate(fp: float, sample_size: int, pilot_sample_rate: float, pilot_sample_size: int):
    if pilot_sample_rate > 0.9:
        bernoulli_N_lb = sample_size
    else:
        bernoulli_N_lb = get_bernoulli_N_lb(pilot_sample_size, pilot_sample_rate, fp)
    assert sample_size < bernoulli_N_lb, f"{sample_size} is too big"
    z_val = norm.ppf(1-fp)
    p = _solve_quadratic(a=bernoulli_N_lb**2 + z_val**2 * bernoulli_N_lb,
                        b=-(2*bernoulli_N_lb*sample_size + z_val**2 * bernoulli_N_lb),
                        c=sample_size**2)[1]
    return p

def get_bernoulli_N_sample_rate(error, fp: float, fp1: float, pilot_sample_rate: float, pilot_sample_size: int):
    bernoulli_N_lb = get_bernoulli_N_lb(pilot_sample_size, pilot_sample_rate, fp1)
    z_val = norm.ppf(1-fp/2)
    return 1 / (1 + error**2 * bernoulli_N_lb / z_val**2)

def estimate_final_rate(failure_prob: float, pilot_results: pd.DataFrame, page_errors: Dict, group_cols: List[str], 
                        pilot_rate: float=0.0001, limit: int|None=None):
    page_stats_cols = [col for col in page_errors.keys() if col != "n_page"]
    n_page_stats = len(page_stats_cols)
    page_size_stats = len(page_errors) - n_page_stats
    keep_columns = group_cols + page_stats_cols
    pilot_results = pilot_results[keep_columns]
    if len(group_cols) > 0:
        if limit is not None:
            df = pilot_results.groupby(by=group_cols, sort=False).agg(["mean", "std", "size"]).head(limit)
        else:
            df = pilot_results.groupby(by=group_cols, sort=False).agg(["mean", "std", "size"])
    else:
        df = pilot_results.agg(["mean", "std", "size"])
    n_groups = df.shape[0] if len(group_cols) > 0 else 1
    n_est = n_groups * (n_page_stats * 3 + page_size_stats*2 + 1)
    candidate_sample_rate = []
    print(df)
    try:
        fp = 1 - math.pow(1-failure_prob, 1/n_est)
        for col, error in page_errors.items():
            if len(group_cols) > 0:
                for group_i in range(n_groups):
                    if col == "n_page":
                        sample_size = df[(page_stats_cols[0], "size")].iloc[group_i]
                        final_sample_rate = get_bernoulli_N_sample_rate(error, fp, fp, pilot_rate, sample_size)
                        candidate_sample_rate.append(final_sample_rate)
                    else:
                        sample_mean = df[(col, 'mean')].iloc[group_i]
                        sample_std = df[(col, 'std')].iloc[group_i]
                        sample_size = df[(col, 'size')].iloc[group_i]
                        final_sample_size = get_mean_sample_size(error, fp, fp, fp, sample_mean, sample_std, sample_size)
                        final_sample_rate = get_sample_rate(fp, final_sample_size, pilot_rate, sample_size)
                        candidate_sample_rate.append(final_sample_rate)
            else:
                if col == "n_page":
                    sample_size = df[page_stats_cols[0]].iloc[2]
                    final_sample_rate = get_bernoulli_N_sample_rate(error, fp, fp, pilot_rate, sample_size)
                    candidate_sample_rate.append(final_sample_rate)
                    print(f"final_sample_rate for {col}: {final_sample_rate}")
                else:
                    sample_mean = df[col].iloc[0]
                    sample_std = df[col].iloc[1]
                    sample_size = df[col].iloc[2]
                    final_sample_size = get_mean_sample_size(error, fp, fp, fp, sample_mean, sample_std, sample_size)
                    final_sample_rate = get_sample_rate(fp, final_sample_size, pilot_rate, sample_size)
                    candidate_sample_rate.append(final_sample_rate)
                    print(f"final_sample_size for {col}: {final_sample_size}")
                    print(f"final_sample_rate for {col}: {final_sample_rate}")

    except Exception as e:
        logging.info(f"fail to estimate final sample rate due to {e}")
        return -1
    return max(candidate_sample_rate)

def estimate_final_rate_uniform(failure_prob: float, pilot_results: pd.DataFrame, 
                                page_errors: Dict, 
                                pilot_rate: float=0.0001):
    try:
        max_sample_rate = 0
        for group_id, row in pilot_results.iterrows():
            for col, error in page_errors.items():
                if col == "size":
                    sample_size = row[col]
                    final_sample_rate = get_bernoulli_N_sample_rate(error, failure_prob, failure_prob, pilot_rate, sample_size)
                    max_sample_rate = max(max_sample_rate, final_sample_rate)
                else:
                    sample_mean = row[col]
                    sample_std = row[col.replace("avg", "std")]
                    sample_size = row["sample_size"]
                    final_sample_size = get_mean_sample_size(error, failure_prob, failure_prob, pilot_rate, sample_mean, sample_std, sample_size)
                    final_sample_rate = get_sample_rate(failure_prob, final_sample_size, pilot_rate, sample_size)
                max_sample_rate = max(max_sample_rate, final_sample_rate)
    except Exception as e:
        logging.info(f"fail to estimate final sample rate due to {e}")
        return -1
    return max_sample_rate

def estimate_final_rate_oracle_tpch1(pilot_results: pd.DataFrame):
    columns = ["avg_1", "avg_2", "avg_3", "avg_4", "avg_5", "avg_6"]
    max_sample_rate = 0
    n_groups = len(pilot_results)
    fp = 1 - math.pow(1-0.05, 1/n_groups/len(columns)/4)
    for it, row in pilot_results.iterrows():
        for col in columns:
            sample_mean = row[col]
            sample_std = row[col.replace("avg", "std")]
            sample_size = row["n_page"]
            final_sample_size = get_mean_sample_size(0.024, fp, fp, fp, sample_mean, sample_std, sample_size)
            final_sample_rate = get_sample_rate(fp, final_sample_size, 1, sample_size)
            max_sample_rate = max(max_sample_rate, final_sample_rate)
        # sample rate for size column
        sample_size = row["n_page"]
        final_sample_rate = get_bernoulli_N_sample_rate(0.024, fp, fp, 1, sample_size)
        max_sample_rate = max(max_sample_rate, final_sample_rate)
    return max_sample_rate
if __name__ == "__main__":
    print(get_sample_rate(0.025, 300, 0.0001, 30))
