
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
    return 1 / (1 + error**2 * bernoulli_N_lb**3 / z_val**2)

def estimate_final_rate(failure_prob: float, pilot_results: pd.DataFrame, page_errors: Dict, group_cols: List[str], pilot_rate: float=0.0001):
    page_stats_cols = [col for col in page_errors.keys() if col != "n_page"]
    n_page_stats = len(page_stats_cols)
    page_size_stats = len(page_errors) - n_page_stats
    if len(group_cols) > 0:
        df = pilot_results.groupby(by=group_cols).agg(["mean", "std", "size"])
    else:
        df = pilot_results.agg(["mean", "std", "size"])
    n_groups = df.shape[0] if len(group_cols) > 0 else 1
    n_est = n_groups * (n_page_stats * 3 + page_size_stats*2 + 1)
    fp = 1 - math.pow(1-failure_prob, 1/n_est)
    candidate_sample_rate = []
    try:
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
                else:
                    sample_mean = df[col].iloc[0]
                    sample_std = df[col].iloc[1]
                    sample_size = df[col].iloc[2]
                    final_sample_size = get_mean_sample_size(error, fp, fp, fp, sample_mean, sample_std, sample_size)
                    final_sample_rate = get_sample_rate(fp, final_sample_size, pilot_rate, sample_size)
                    candidate_sample_rate.append(final_sample_rate)

    except Exception as e:
        logging.info(f"fail to estimate final sample rate due to {e}")
        return -1
    return max(candidate_sample_rate)

if __name__ == "__main__":
    print(get_sample_rate(0.025, 300, 0.0001, 30))