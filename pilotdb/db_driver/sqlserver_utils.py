import pyodbc
import pandas as pd
import pandas.io.sql as sqlio

def connect_to_db(db: str, user: str, host: str="localhost", password: str|None=None):
    conn_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={host};DATABASE={db};UID={user};PWD={password};TrustServerCertificate=yes;'
    conn = pyodbc.connect(conn_string)
    return conn

def close_connection(conn):
    conn.close()

def execute_query(conn, query: str) -> pd.DataFrame:
    return sqlio.read_sql_query(query, conn)
