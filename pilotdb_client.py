import argparse
import requests
import yaml
import json

AQP_URL = "http://localhost:8080/run_aqp_query"
EXACT_URL = "http://localhost:8080/run_exact_query"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PilotDB Client")
    parser.add_argument("--mode", type=str, default="aqp", help="The host of the PilotDB server")
    parser.add_argument("--query_file", type=str, default="example/tpch-6.sql", help="The query to run")
    parser.add_argument("--db_config_file", type=str, default="example/duckdb_config.yml", help="The database to query")
    parser.add_argument("--error", type=float, default=0.05, help="Error threshold for AQP")
    parser.add_argument("--probability", type=float, default=0.05, help="Failure probability for the error threshold")
    args = parser.parse_args()
    
    with open(args.query_file, 'r') as file:
        query = file.read().strip()
    
    with open(args.db_config_file, 'r') as file:
        db_config = yaml.safe_load(file)

    payload = {
        "query": query,
        "db_config": db_config,
        "error": args.error,
        "probability": args.probability,
        "database": db_config.get("database", "duckdb")
    }
    
    BASE_URL = AQP_URL if args.mode == "aqp" else EXACT_URL

    print(payload)
    
    try:
        response = requests.post(BASE_URL, json=payload)
        response.raise_for_status()
        result = response.json()
        print(json.dumps(result, indent=2))
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")