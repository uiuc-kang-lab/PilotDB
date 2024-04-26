import pilotdb.db_driver.duckdb_utils as duckdb_utils
import pilotdb.db_driver.postgres_utils as postgres_utils

import pandas as pd
import yaml


def connect_to_db(dbms: str, config_file: str):
    with open(config_file) as f:
        config = yaml.safe_load(f)

    if dbms == 'duckdb':
        return duckdb_utils.connect_to_db(config["path"])
    elif dbms == 'postgres':
        return postgres_utils.connect_to_db(config["db"], config["user"], config["host"], config["port"], config["password"])
    else:
        raise ValueError(f"Unknown DBMS: {dbms}")
    
def close_connection(conn, dbms: str):
    if dbms == 'duckdb':
        return duckdb_utils.close_connection(conn)
    elif dbms == 'postgres':
        return postgres_utils.close_connection(conn)
    else:
        raise ValueError(f"Unknown DBMS: {dbms}")

def execute_query(conn, query: str, dbms: str) -> pd.DataFrame:
    if dbms == 'duckdb':
        return duckdb_utils.execute_query(conn, query)
    elif dbms == 'postgres':
        return postgres_utils.execute_query(conn, query)
    else:
        raise ValueError(f"Unknown DBMS: {dbms}")

def get_sampling_clause(rate: float, dbms: str) -> str:
    if dbms == "duckdb":
        return f"TABLESAMPLE SYSTEM({rate}%)"
    elif dbms == "postgres":
        return f"TABLESAMPLE SYSTEM ({rate})"
    else:
        ValueError(f"Unknown DBMS: {dbms}")