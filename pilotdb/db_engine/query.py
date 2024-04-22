import pilotdb.db_engine.duckdb_utils as duckdb_utils
import pilotdb.db_engine.postgres_utils as postgres_utils

import pandas as pd

def connect_to_db(dbms: str, **kwargs):
    if dbms == 'duckdb':
        return duckdb_utils.connect_to_db(**kwargs)
    elif dbms == 'postgres':
        return postgres_utils.connect_to_db(**kwargs)
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
