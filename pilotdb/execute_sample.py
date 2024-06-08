from pilotdb.query import *
from pilotdb.utils.path import *
from pilotdb.utils.timer import Timer
from pilotdb.db_driver.driver import *
from pilotdb.pilot_engine.commons import *
from pilotdb.pilot_engine.rewriter.pilot import Pilot_Rewriter
from pilotdb.pilot_engine.error_bounds import estimate_final_rate
from pilotdb.pilot_engine.rewriter.sampling import Sampling_Rewriter
from pilotdb.pilot_engine.utils import aggregate_error_to_page_error
from pilotdb.utils.utils import setup_logging, dump_results, get_largest_sample_rate

import json
import time
import logging
import pandas as pd
from typing import Dict
from sqlglot import transpile

def execute_sample(query: Query, sample_rate: float, db_config: dict):
    dbms = db_config["dbms"]
    conn = connect_to_db(dbms, db_config)
    sq = Sampling_Rewriter(query.table_cols, query.table_size, dbms)
    sampling_query = sq.rewrite(query.query) + ";"

    # subquery_results = process_subqueries(dbms, conn, sq)
    sampling_query = sampling_query.format(sampling_method=get_sampling_clause(sample_rate*100, dbms), 
                                           sample_rate=sample_rate)
    # for subquery_name, subquery_result in subquery_results.items():
    #     sampling_query = sampling_query.replace(subquery_name, subquery_result)
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
      result = {"query": query.name, "dbms": dbms, "final_sample_rate": sample_rate, "sample only execution time": runtime,
                "results_file": result_file}
      f.write(json.dumps(result) + "\n")

    return results_df, {"sample_only_runtime": runtime}