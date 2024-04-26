import warnings
import argparse
warnings.simplefilter(action='ignore', category=UserWarning)

from pilotdb.execute import execute_aqp, execute_exact

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run pilotdb on a benchmark query')
    parser.add_argument('--benchmark', type=str, default="tpch", help='benchmark name')
    parser.add_argument('--qid', type=str, default="1", help='query id')
    parser.add_argument('--pilot_sample_rate', type=float, default=0.05, help='pilot sample rate')
    parser.add_argument('--dbms', type=str, default='postgres', help='dbms')
    parser.add_argument('--db_config_file', type=str, default='db_configs/postgres.yml', help='db config file')
    parser.add_argument('--process_mode', type=str, default='aqp', help='query mode')
    args = parser.parse_args()
    
    if args.process_mode == "aqp":
        execute_aqp(args.benchmark, args.qid, args.pilot_sample_rate, args.dbms, args.db_config_file)
    elif args.process_mode == "exact":
        execute_exact(args.benchmark, args.qid, args.dbms, args.db_config_file)