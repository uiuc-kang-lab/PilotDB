import os
import json
import logging
import pandas as pd


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
    
    result_dict = results_df.to_dict()
    with open(result_file, "a+") as f:
        json.dump(result_dict, f)
    
