import psycopg2
import pandas as pd
import pandas.io.sql as sqlio

def connect_to_db(db: str, user: str, host: str="localhost", port: int=5432, password: str|None=None):
    conn = psycopg2.connect(dbname=db, user=user, host=host, port=port, password=password)
    return conn

def close_connection(conn):
    conn.close()

def execute_query(conn, query: str) -> pd.DataFrame:
    return sqlio.read_sql_query(query, conn)
