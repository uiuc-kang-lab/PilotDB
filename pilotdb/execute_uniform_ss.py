from pilotdb.query import *
from pilotdb.utils.path import *
from pilotdb.utils.timer import Timer
from pilotdb.db_driver.driver import *
from pilotdb.pilot_engine.commons import *
from pilotdb.pilot_engine.rewriter.pilot import Pilot_Rewriter
from pilotdb.pilot_engine.error_bounds import estimate_final_rate_uniform
from pilotdb.pilot_engine.rewriter.sampling import Sampling_Rewriter
from pilotdb.pilot_engine.utils import aggregate_error_uniform
from pilotdb.utils.utils import setup_logging, dump_results, get_largest_sample_rate

import json
import time
import logging
import pandas as pd
from typing import Dict
from sqlglot import transpile
import importlib.util
import sys
import math

def uniform_rewriter(dbms: str, query_name: str):
    spec = importlib.util.spec_from_file_location("query_rewriter", f"benchmarks/{dbms}/uniform/{query_name}.py")
    query_rewriter = importlib.util.module_from_spec(spec)
    sys.modules["query_rewriter"] = query_rewriter
    spec.loader.exec_module(query_rewriter)
    return query_rewriter

def execute_uniform_ss(query: Query, db_config: dict, sample_rate: float):
    dbms = db_config["dbms"]
    conn = connect_to_db(dbms, db_config)
    sq = uniform_rewriter(dbms, query.name)
    sampling_query = sq.sampling_query
    
    if sample_rate != 100:
        if sample_rate < 0.05:
            rate = sample_rate * 1000000
        else:
            rate = sample_rate * 10000
        rate = math.ceil(rate)
        
        sampling_clause = get_uniform_sampling_clause(rate, dbms)
        sampling_query = sampling_query.format(sampling_method=sampling_clause, 
                                            sample_rate=rate/1000000)
    else:
        sampling_query = query.query
    start = time.time()
    job_id = str(int(start*100))
    log_file = f"logs/{query.name}-{job_id}.log"
    setup_logging(log_file=log_file)
    
    logging.info(f"sample query: {sampling_query}")

    results_df = execute_query(conn, sampling_query, dbms)
    runtime = time.time() - start

    close_connection(conn, dbms)
    result_file = f"results/{query.name}-sample_only-{dbms}-{job_id}.csv"
    dump_results(result_file=result_file, results_df=results_df)

    logging.info(f"sample only execution time: {runtime}")
    logging.info(f"sample only result:\n{results_df}")
    
    with open("all_results.jsonl", "a+") as f:
      result = {"query": query.name, "dbms": dbms, "final_sample_rate": rate/1000000, "sample only execution time": runtime,
                "results_file": result_file}
      f.write(json.dumps(result) + "\n")

    return results_df, {"sample_only_runtime": runtime}