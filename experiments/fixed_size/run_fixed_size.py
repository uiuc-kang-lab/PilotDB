from pilotdb.db_driver.driver import connect_to_db, close_connection
from typing import List
import json
import argparse
import yaml
import time

def run(queries: List[str], config: dict):
    conn = connect_to_db("postgres", config)
    with conn.cursor() as cursor:
        for query in queries:
            cursor.execute(query)
            result = cursor.fetchall()
            print(result)
    close_connection(conn, "postgres")


def run_order_by_random_rows(query_id: str, config: dict):
    with open(f"tpch_postgres_order_by_random_rows/query_{query_id}.sql") as f:
        query_template = f.read()
    with open("tpch_postgres_order_by_random_rows/meta.json") as f:
        meta = json.load(f)
    table_size = meta["table_sizes"][query_id]
    sample_rate = meta["sample_rates"][query_id]
    sample_size = int(table_size * sample_rate / 100)
    pilot_sample_size = int(table_size * 0.05 / 100)
    pilot_query = query_template.format(sample_size=pilot_sample_size)
    final_query = query_template.format(sample_size=sample_size)
    queries = [pilot_query, final_query]
    run(queries, config)

def run_tsm_system_rows(query_id: str, config: dict):
    with open(f"tpch_postgres_tsm_system_rows/query_{query_id}.sql") as f:
        query_template = f.read()
    with open("tpch_postgres_tsm_system_rows/meta.json") as f:
        meta = json.load(f)
    table_size = meta["table_sizes"][query_id]
    sample_rate = meta["sample_rates"][query_id]
    sample_size = int(table_size * sample_rate / 100)
    pilot_sample_size = int(table_size * 0.05 / 100)
    pilot_query = query_template.format(sample_size=pilot_sample_size)
    final_query = query_template.format(sample_size=sample_size)
    init_query = "CREATE EXTENSION tsm_system_rows;"
    queries = [init_query, pilot_query, final_query]
    run(queries, config)
    
def run_exact(query_id: str, config: dict):
    with open(f"tpch_postgres/query_{query_id}.sql") as f:
        query = f.read()
    queries = [query]
    run(queries, config)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--qid", type=str, default="1")
    parser.add_argument("--process_mode", type="str", default="exact")
    parser.add_argument("--db_config_file", type="str", default="db_configs/postgres_tpch.yml")
    args = parser.parse_args()
    
    with open(args.db_config_file) as f:
        config = yaml.safe_load(f)
    
    start = time.time()
    if args.process_mode == "order_by_random_rows":
        run_order_by_random_rows(args.qid, config)
    elif args.process_mode == "tsm_system_rows":
        run_tsm_system_rows(args.qid, config)
    elif args.process_mode == "exact":
        run_exact(args.qid, config)
    else:
        raise ValueError(f"{args.process_mode} is not in [order_by_random_rows, tsm_system_rows, exact]")