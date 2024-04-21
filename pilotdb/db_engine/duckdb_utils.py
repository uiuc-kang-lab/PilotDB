import duckdb
from decimal import Decimal

def connect_to_db(path: str):
    conn = duckdb.connect(database=path, read_only=False)
    return conn

def close_connection(conn):
    conn.close()

def execute_query(conn, query: str):
    return conn.execute(query).fetchdf()
