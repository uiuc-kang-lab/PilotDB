import os
import xml.etree.ElementTree as ET
from xml.dom import minidom

import pandas as pd
import pandas.io.sql as sqlio
import pyodbc


def connect_to_db(db: str, user: str, host: str="127.0.0.1", password: str|None=None):
    conn_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={host};DATABASE={db};UID={user};PWD={password};TrustServerCertificate=yes;'
    conn = pyodbc.connect(conn_string)
    conn.execute('DBCC FREEPROCCACHE;')
    conn.execute('DBCC DROPCLEANBUFFERS;')
    return conn

def close_connection(conn):
    conn.close()

def execute_query(conn, query: str) -> pd.DataFrame:
    return sqlio.read_sql_query(query, conn)


def is_index_seek(conn, query: str, largest_table: str):
    with conn.cursor() as cursor:
        # Enable showplan XML to get the query plan without executing the query
        cursor.execute("SET SHOWPLAN_XML ON;")
        cursor.commit()
        
        # Execute the query for which we want the plan
        cursor.execute(query)
        
        # Fetch the plan
        plan = cursor.fetchone()

        # Disable showplan XML to return to normal operations
        cursor.execute("SET SHOWPLAN_XML OFF;")
        cursor.commit()

        root = ET.fromstring(plan[0])
        namespaces = {'sql': 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'}

        # Find all elements where PhysicalOp is 'Clustered Index Seek'
        clustered_index_seeks = root.findall(".//sql:RelOp[@PhysicalOp='Clustered Index Seek']", namespaces)
        for node in clustered_index_seeks:
            object_details = node.find('.//sql:Object', namespaces)
            if object_details is not None:
                if largest_table in object_details.get('Table'):
                    return True

        return False
    