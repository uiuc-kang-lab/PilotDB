import os
import json
import logging
import pandas as pd


def setup_logging(log_file: str):
    if not os.path.isdir(log_file):
        os.makedirs(log_file, exist_ok=True)

    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
        datefmt='(%m-%d) %H:%M:%S',
        level=logging.INFO,
        handlers=[
            logging.FileHandler(log_file, mode='a'),
            logging.StreamHandler()
        ])

def dump_results(result_file: str, results_df: pd.DataFrame):
    if not os.path.isdir(result_file):
        os.makedirs(result_file, exist_ok=True)
    
    result_dict = results_df.to_dict(index=False)
    with open(result_file, "a") as f:
        json.dump(result_dict, f)
    
