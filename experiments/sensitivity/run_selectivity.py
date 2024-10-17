import warnings
import argparse
import json
import yaml

warnings.simplefilter(action="ignore", category=UserWarning)

from pilotdb.execute import execute_aqp, execute_exact
from pilotdb.query import Query

from query_sqlserver import query_sel, preds

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run correctness experiments for pilotdb"
    )
    parser.add_argument("--query", type=str, default="tpch-6", help="query to run")
    parser.add_argument(
        "--pilot_sample_rate", type=float, default=0.05, help="pilot sample rate"
    )
    parser.add_argument("--process_mode", type=str, default="aqp", help="query mode")
    parser.add_argument("--error", type=float, default=0.01, help="error rate")
    parser.add_argument(
        "--failure_probability", type=float, default=0.05, help="failure probability"
    )
    parser.add_argument("--selectivity", type=int, default=3, help="selectivity")

    args = parser.parse_args()

    query_str = query_sel.format(pred=preds[args.selectivity - 1])

    meta_path = "../../benchmarks/sqlserver/tpch/meta.json"
    db_config_path = "../../db_configs/sqlserver_tpch.yml"

    with open(meta_path, "r") as f:
        meta = json.load(f)

    query = Query(
        name=args.query,
        query=query_str,
        table_cols=meta["table_cols"],
        table_size=meta["table_size"],
        error=args.error,
        failure_probability=args.failure_probability,
    )

    with open(db_config_path, "r") as f:
        db_config = yaml.safe_load(f)

    if args.process_mode == "aqp":
        execute_aqp(query, db_config, args.pilot_sample_rate)
    elif args.process_mode == "exact":
        execute_exact(query, db_config)
