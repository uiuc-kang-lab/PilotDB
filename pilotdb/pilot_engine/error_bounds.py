
from scipy.stats import t, chi2, norm
def get_mean_ub(sample_size: int, sample_mean: float, sample_std: float, failure_probability: float):
    t_val = t.ppf(1-failure_probability, sample_size-1)
    return t_val * sample_std / (sample_size ** 0.5)

def get_mean_lb(sample_size: int, sample_mean: float, sample_std: float, failure_probability: float):
    t_val = t.ppf(failure_probability, sample_size-1)
    return t_val * sample_std / (sample_size ** 0.5)

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
    p_lb = sample_rate - z_val * (sample_rate * (1-sample_rate) / sample_size) ** 0.5
    return sample_size / p_lb

def get_bernoulli_N_lb(sample_size: int, sample_rate: float, failure_probability: float):
    z_val = norm.ppf(failure_probability)
    p_ub = sample_rate + z_val * (sample_rate * (1-sample_rate) / sample_size) ** 0.5
    return sample_size / p_ub

def get_bernoulli_N_two_side(sample_size: int, sample_rate: float, failure_probability: float):
    failure_probability = failure_probability / 2
    lb = get_bernoulli_N_lb(sample_size, sample_rate, failure_probability)
    ub = get_bernoulli_N_ub(sample_size, sample_rate, failure_probability)
    return lb, ub
