import pilotdb.db_driver.duckdb_utils as duckdb_utils
import pilotdb.db_driver.postgres_utils as postgres_utils
import pilotdb.db_driver.sqlserver_utils as sqlserver_utils
import os
import pandas as pd


def connect_to_db(dbms: str, config: dict):
    if dbms == 'duckdb':
        # clean cache
        if config["flush_memory"] == True:
            os.system("sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches")
        return duckdb_utils.connect_to_db(config["path"])
    elif dbms == 'postgres':
        return postgres_utils.connect_to_db(config["dbname"], config["username"], config["host"], config["port"], 
                                            config["password"])
    elif dbms == 'sqlserver':
        return sqlserver_utils.connect_to_db(config["dbname"], config["username"], config["host"], config["password"])
    else:
        raise ValueError(f"Unknown DBMS: {dbms}")
    
def close_connection(conn, dbms: str):
    if dbms == 'duckdb':
        return duckdb_utils.close_connection(conn)
    elif dbms == 'postgres':
        return postgres_utils.close_connection(conn)
    elif dbms == 'sqlserver':
        return sqlserver_utils.close_connection(conn)
    else:
        raise ValueError(f"Unknown DBMS: {dbms}")

def execute_query(conn, query: str, dbms: str) -> pd.DataFrame:
    if dbms == 'duckdb':
        return duckdb_utils.execute_query(conn, query)
    elif dbms == 'postgres':
        return postgres_utils.execute_query(conn, query)
    elif dbms == 'sqlserver':
        return sqlserver_utils.execute_query(conn, query)
    else:
        raise ValueError(f"Unknown DBMS: {dbms}")

def get_sampling_clause(rate: float, dbms: str) -> str|None:
    if dbms == "duckdb":
        return f"TABLESAMPLE CHUNK({rate}%)"
    elif dbms == "postgres":
        return f"TABLESAMPLE SYSTEM ({rate})"
    elif dbms == "sqlserver":
        return f"TABLESAMPLE ({rate} PERCENT)"
    else:
        ValueError(f"Unknown DBMS: {dbms}")
        
def directly_run_exact(conn, query: str, pilot_query: str, dbms: str, largest_table: str):
    if dbms == 'sqlserver':
        return sqlserver_utils.is_index_seek(conn, query, largest_table)
    elif dbms == 'postgres':
        return postgres_utils.is_high_estimated_cost(conn, query, pilot_query)