import logging
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class Query:
    query: str                          # a query written in SQL
    table_cols: Dict[str, List[str]]    # necessary metainfo: the column names of each table in the DB
    table_size: Dict[str, int]          # necessary metainfo: the number of rows in each table in the DB
    error: float = 0.05                 # the acceptable error rate
    failure_probability: float = 0.05   # the acceptable failure probability
    name: str = "query"                 # the name of the query

def log_query_info(query: Query, dbms: str="unknown"):
    logging.info(f"start approximately processing query {query.name} on {dbms}")
    logging.info(f"original:\n{query.query}")
