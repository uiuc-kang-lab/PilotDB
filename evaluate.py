import warnings
import argparse
import json
import yaml

warnings.simplefilter(action="ignore", category=UserWarning)

from pilotdb.execute import (
    execute_aqp,
    execute_exact,
    execute_oracle_aqp,
    execute_uniform,
    execute_block_wrong,
    execute_sample,
)
from pilotdb.query import Query

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Evaluate PilotDB on a benchmark query"
    )
    parser.add_argument("--benchmark", type=str, default="tpch", help="benchmark name")
    parser.add_argument("--qid", type=str, default="1", help="query id")
    parser.add_argument(
        "--pilot_sample_rate", type=float, default=0.05, help="pilot sample rate"
    )
    parser.add_argument("--dbms", type=str, default="postgres", help="dbms")
    parser.add_argument(
        "--db_config_file",
        type=str,
        default="db_configs/postgres_tpch.yml",
        help="db config file",
    )
    parser.add_argument("--process_mode", type=str, default="aqp", help="query mode")
    parser.add_argument("--error", type=float, default=0.05, help="error rate")
    parser.add_argument(
        "--failure_probability", type=float, default=0.05, help="confidence"
    )
    parser.add_argument(
        "--sample_rate",
        type=float,
        default=0.05,
        help="sample rate of the largest table (valid only in 'sample' process mode)",
    )

    args = parser.parse_args()

    with open(
        f"benchmarks/{args.dbms}/{args.benchmark}/query_{args.qid}.sql", "r"
    ) as f:
        query_str = f.read()

    with open(f"benchmarks/{args.dbms}/{args.benchmark}/meta.json", "r") as f:
        meta = json.load(f)

    query = Query(
        name=f"{args.benchmark}-{args.qid}",
        query=query_str,
        table_cols=meta["table_cols"],
        table_size=meta["table_size"],
        error=args.error,
        failure_probability=args.failure_probability,
    )

    with open(args.db_config_file, "r") as f:
        db_config = yaml.safe_load(f)

    if args.process_mode == "aqp":
        execute_aqp(query, db_config, args.pilot_sample_rate)
    elif args.process_mode == "aqp-oracle":
        execute_oracle_aqp(query, db_config)
    elif args.process_mode == "aqp-uniform":
        execute_uniform(query, db_config)
    elif args.process_mode == "aqp-nobsap":
        execute_block_wrong(query, db_config, args.pilot_sample_rate)
    elif args.process_mode == "exact":
        execute_exact(query, db_config)
    elif args.process_mode == "sample":
        execute_sample(query, args.sample_rate, db_config)
