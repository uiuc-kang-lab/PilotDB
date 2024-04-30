import os
import json
import logging
import pandas as pd

from pilotdb.pilot_engine.commons import *

def setup_logging(log_file: str):
    log_dir = os.path.dirname(log_file)
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
        datefmt='(%m-%d) %H:%M:%S',
        level=logging.INFO,
        handlers=[
            logging.FileHandler(log_file, mode='a+'),
            logging.StreamHandler()
        ])

def dump_results(result_file: str, results_df: pd.DataFrame):
    result_dir = os.path.dirname(result_file)
    if not os.path.isdir(result_dir):
        os.makedirs(result_dir, exist_ok=True)
    
    results_df.to_csv(result_file)


def get_largest_sample_rate(dbms: str) -> float:
    if dbms == POSTGRES:
        return 5
    elif dbms == DUCKDB:
        return 75
    elif dbms == SQLSERVER:
        return 10
    else:
        return 5