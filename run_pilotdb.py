import warnings
import argparse
import json
import yaml

warnings.simplefilter(action='ignore', category=UserWarning)

from pilotdb.execute import execute_aqp, execute_exact
from pilotdb.execute_oracle import execute_oracle_aqp
from pilotdb.execute_uniform import execute_uniform
from pilotdb.execute_uniform_ss import execute_uniform_ss
from pilotdb.execute_sample import execute_sample
from pilotdb.query import Query

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run pilotdb on a benchmark query')
    parser.add_argument('--benchmark', type=str, default="tpch", help='benchmark name')
    parser.add_argument('--qid', type=str, default="1", help='query id')
    parser.add_argument('--pilot_sample_rate', type=float, default=0.05, help='pilot sample rate')
    parser.add_argument('--dbms', type=str, default='postgres', help='dbms')
    parser.add_argument('--db_config_file', type=str, default='db_configs/postgres_tpch.yml', help='db config file')
    parser.add_argument('--process_mode', type=str, default='aqp', help='query mode')
    parser.add_argument('--error', type=float, default=0.05, help='error rate')
    parser.add_argument('--failure_probability', type=float, default=0.05, help='failure probability')

    args = parser.parse_args()

    with open(f"benchmarks/{args.dbms}/{args.benchmark}/query_{args.qid}.sql", "r") as f:
        query_str = f.read()
    
    with open(f"benchmarks/{args.dbms}/{args.benchmark}/meta.json", "r") as f:
        meta = json.load(f)

    query = Query(name=f"{args.benchmark}-{args.qid}", query=query_str,
                  table_cols=meta["table_cols"], table_size=meta["table_size"],
                  error=args.error, failure_probability=args.failure_probability)
    
    with open(args.db_config_file, "r") as f:
        db_config = yaml.safe_load(f)

    if args.process_mode == "aqp":
        execute_aqp(query, db_config, args.pilot_sample_rate)
    elif args.process_mode == "exact":
        execute_exact(query, db_config)
    elif args.process_mode == "oracle":
        execute_oracle_aqp(query, db_config)
    elif args.process_mode == "uniform":
        if db_config['dbms'] == 'sqlserver':
            with open(f"benchmarks/{args.dbms}/uniform/uniform_sample_rate.jsonl", "r") as f:
                sample_rate_list = json.load(f)
            for sample_rate in sample_rate_list[f"{args.benchmark}-{args.qid}"]:
                execute_uniform_ss(query, db_config, sample_rate)
        else:
            execute_uniform(query, db_config)
    elif args.process_mode == "sample":
        with open(f"./sample_rate.json", "r") as f:
            meta = json.load(f)
        sample_rate_list = meta[db_config["dbms"]][args.benchmark][f'query_{args.qid}']
        for sample_rate in sample_rate_list:
            execute_sample(query, sample_rate, db_config)