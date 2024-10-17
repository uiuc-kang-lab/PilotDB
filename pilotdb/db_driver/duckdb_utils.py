import duckdb
import pandas as pd


def connect_to_db(path: str):
    conn = duckdb.connect(database=path, read_only=False)
    return conn


def close_connection(conn):
    conn.close()


def execute_query(conn, query: str) -> pd.DataFrame:
    return conn.execute(query).fetchdf()
