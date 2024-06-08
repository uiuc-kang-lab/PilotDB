import duckdb
import scipy
import numpy as np
import pandas as pd

def N_CI(delta: float, theta: float, n):
    z = scipy.stats.norm.ppf(1 - delta / 2)
    b = z * np.sqrt(1/theta - 1)
    c = n / theta
    return ((-b + np.sqrt(b**2 + 4 * c)) / 2)**2, ((b + np.sqrt(b**2 + 4 * c)) / 2)**2


def run_exp(args):
    conn = duckdb.connect(args.db_path)
    df = conn.sql("SELECT COUNT(*) FROM orders WHERE o_comment LIKE '%special%';").df()
    gt = df.iloc[0,0]

    coverage = []
    target_errors = []
    achieved_errors = []
    for _ in range(args.repeats):
        query = f"SELECT COUNT(*) FROM orders TABLESAMPLE SYSTEM({args.sample_rate}%) WHERE o_comment LIKE '%special%';"
        n = conn.sql(query).df().iloc[0,0]
        ci_lower, ci_upper = N_CI(0.05, args.sample_rate/100, n)
        covered = ci_lower <= gt <= ci_upper
        target_error = abs(ci_upper - ci_lower) / gt / 2
        achieved_error = abs(n / (args.sample_rate/100) - gt) / gt
        print(f"Covered: {covered}, Target error: {target_error}, Achieved error: {achieved_error}")
        coverage.append(covered)
        target_errors.append(target_error)
        achieved_errors.append(achieved_error)
    print(f"Sample rate: {args.sample_rate}")
    print(f"Coverage: {coverage}")
    print(f"Target error: {target_errors}")
    print(f"Achieved error: {achieved_errors}")
    data = {
        "coverage": coverage,
        "target_error": target_errors,
        "achieved_error": achieved_errors
    }
    df = pd.DataFrame(data)
    df.to_csv(f"motivating_{args.sample_rate}.csv", index=False)

if __name__ == "__main__":
    import argparse 
    parser = argparse.ArgumentParser(description='Run correctness experiments for pilotdb')
    parser.add_argument('--db_path', type=str, default="../../../.cache/aqp/tpch_1t.duckdb", help='path to db')
    parser.add_argument('--sample_rate', type=float, default=0.1, help="sample rate")
    parser.add_argument('--repeats', type=int, default=100, help="number of repeatitions")

    args = parser.parse_args()

    run_exp(args)
