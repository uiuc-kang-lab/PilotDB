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

def uniform_rewriter(dbms: str, query_name: str):
    spec = importlib.util.spec_from_file_location("query_rewriter", f"benchmarks/{dbms}/uniform/{query_name}.py")
    query_rewriter = importlib.util.module_from_spec(spec)
    sys.modules["query_rewriter"] = query_rewriter
    spec.loader.exec_module(query_rewriter)
    return query_rewriter

def execute_uniform(query: Query, db_config: dict, pilot_sample_rate: float=0.05):
    # prepare the query and db
    dbms = db_config["dbms"]
    conn = connect_to_db(dbms, db_config)
    
    # FIXME: query rewriter for uniform sampling

    if dbms == SQLSERVER:
        pq = uniform_rewriter(dbms, query.name)
        pilot_query = pq.pilot_query + ";"
        sampling_query = pq.sampling_query
        sampling_clause = get_uniform_sampling_clause(pilot_sample_rate, dbms)
        pilot_query = pilot_query.format(sampling_method=sampling_clause)
    else:
        pq = uniform_rewriter(dbms, query.name)
        pilot_query = pq.pilot_query + ";"
        sq = Sampling_Rewriter(query.table_cols, query.table_size, dbms)
        sampling_query = sq.rewrite(query.query) + ";"
        sampling_clause = get_uniform_sampling_clause(pilot_sample_rate, dbms)
        pilot_query = pilot_query.format(sampling_method=sampling_clause)

    # start execution
    timer = Timer()
    job_id = str(int(timer.start()*100))
    setup_logging(log_file=get_log_file_path("logs", query.name, job_id))
    log_query_info(query, dbms)

    subquery_results = process_subqueries(dbms, conn, pq)

    # execute pilot query
    for subquery_name, subquery_result in subquery_results.items():
        pilot_query = pilot_query.replace(subquery_name, subquery_result)
    
    if dbms != 'sqlserver':
        logging.info(f"pilot query:\n{transpile(pilot_query, read=dbms, pretty=True)[0]}")
    else:
        logging.info(f"pilot query {pilot_query}")

    pilot_results = execute_query(conn, pilot_query, dbms)
    # dump_results(result_file=get_result_file_path("./results", query.name, job_id, "pilot", dbms), 
    #              results_df=pilot_results)
    logging.info(f"pilot query executing time: {timer.check('pilot_query_execution')}")

    # parse the results of pilot query
    errors = aggregate_error_uniform(pq.results_mapping, required_error=query.error)
    logging.info(f"converted page errors: {errors}")
    final_sample_rate = estimate_final_rate_uniform(failure_prob=query.failure_probability, pilot_results=pilot_results, page_errors=errors,
                                                    pilot_rate=pilot_sample_rate/100)
    logging.info(f"sample rate solving time: {timer.check('sampling_rate_solving')}")

    if final_sample_rate == -1:
        final_sample_rate = 1
        logging.info(f"fail to solve sample rate, fall back to original queries")
    elif final_sample_rate*100 > get_largest_sample_rate(dbms):
        logging.info(f"too big sample rate {final_sample_rate*100}, fall back to original queries")
        final_sample_rate = 1
    if final_sample_rate == 1:
        # sampling_query = sampling_query.format(sampling_method="", sample_rate="1")
        # for subquery_name, subquery_result in subquery_results.items():
        #     sampling_query = sampling_query.replace(subquery_name, subquery_result)
        sampling_query = query.query
        logging.info(f"sampling query:\n{sampling_query}")
        results_df = execute_query(conn, sampling_query, dbms)
        logging.info(f"sampling execution time: {timer.check('sampling_query_execution')}")
    elif final_sample_rate*100 > pilot_sample_rate:
        final_sample_rate = round(final_sample_rate*100, 2)
        logging.info(f"final sample rate: {final_sample_rate}")
        sampling_clause = get_uniform_sampling_clause(final_sample_rate, dbms)
        sampling_query = sampling_query.format(sampling_method=sampling_clause, sample_rate=final_sample_rate/100)
        for subquery_name, subquery_result in subquery_results.items():
            sampling_query = sampling_query.replace(subquery_name, subquery_result)
        logging.info(f"sampling query:\n{sampling_query}")
        results_df = execute_query(conn, sampling_query, dbms)
        logging.info(f"sampling execution time: {timer.check('sampling_query_execution')}")
    else:
        logging.info(f"final sample rate: {final_sample_rate}, pilot sampling is large enough")
        # FIXME: directly translate pilot results instead of running sampling again
        sampling_clause = get_uniform_sampling_clause(pilot_sample_rate, dbms)
        sampling_query = sampling_query.format(sampling_method=sampling_clause, sample_rate=pilot_sample_rate/100)
        for subquery_name, subquery_result in subquery_results.items():
            sampling_query = sampling_query.replace(subquery_name, subquery_result)
        results_df = execute_query(conn, sampling_query, dbms)
        logging.info(f"sampling execution time: {timer.check('sampling_query_execution')}")

    timer.stop()
    close_connection(conn, dbms)
    
    logging.info(f"aqp result:\n{results_df}")
    dump_results(result_file=get_result_file_path("./results", query.name, job_id, "uniform", dbms), 
                 results_df=results_df)
    
    with open("all_results.jsonl", "a+") as f:
        result = {"query": query.name, "dbms": dbms, "pilot_sample_rate": pilot_sample_rate, "final_sample_rate": final_sample_rate,
                  "runtime": timer.get_records(),
                  "error": query.error, "failure_probability": query.failure_probability,
                  "results_file": get_result_file_path("./results", query.name, job_id, "uniform", dbms)}
        f.write(json.dumps(result) + "\n")

    return results_df, timer.get_records()

def process_subqueries(dbms, conn, pq) -> Dict[str, str]:
    subquery_results = {}
    if len(pq.subquery_dict) != 0:
        for subquery_name, subquery in pq.subquery_dict.items():
            subquery_result = execute_query(conn, subquery, dbms)
            # subquery should only have one column
            assert len(subquery_result.columns) == 1
            column_name = subquery_result.columns[0]
            if len(subquery_result[column_name]) != 1:
                # convert the subquery results into a list
                subquery_result = subquery_result[column_name].tolist()
                # format the subquery results
                if isinstance(subquery_result[0], str) or isinstance(subquery_result[0], pd.Timestamp):
                    subquery_result = [f"'{r}'" for r in subquery_result]
                else:
                    subquery_result = [str(r) for r in subquery_result]
                subquery_result = "( " + ", ".join(subquery_result) + " )"
            else:
                subquery_result = subquery_result[column_name][0]
                if isinstance(subquery_result, str):
                    subquery_result = f"'{subquery_result}'"
                else:
                    subquery_result = str(subquery_result)
            subquery_results[subquery_name] = subquery_result
    return subquery_results
    

def execute_exact(query: Query, db_config: dict):
    dbms = db_config["dbms"]
    conn = connect_to_db(dbms, db_config)
    timer = Timer()
    job_id = str(int(timer.start()*100))
    log_file = f"logs/{query.name}-{job_id}.log"
    setup_logging(log_file=log_file)
    results_df = execute_query(conn, query.query, dbms)
    logging.info(f"exact execution time: {timer.check('exact_execution')}")
    timer.stop()
    close_connection(conn, dbms)
    dump_results(result_file=get_result_file_path("./results", query.name, job_id, "exact", dbms), 
                 results_df=results_df)
    logging.info(f"exact result:\n{results_df}")

    with open("all_results.jsonl", "a+") as f:
        result = {"query": query.name, "dbms": dbms, "runtime": timer.get_records(),
                  "error": query.error, "failure_probability": query.failure_probability,
                  "results_file": get_result_file_path("./results", query.name, job_id, "exact", dbms)}
        f.write(json.dumps(result) + "\n")

    return results_df, timer.get_records()

def execute_sample_only(query: Query, sample_rate: float, db_config: dict):
    dbms = db_config["dbms"]
    conn = connect_to_db(dbms, db_config)
    sq = Sampling_Rewriter(query.table_cols, query.table_size, dbms)
    sampling_query = sq.rewrite(query.query) + ";"

    subquery_results = process_subqueries(dbms, conn, sq)
    sampling_query = sampling_query.format(sampling_method=get_sampling_clause(sample_rate, dbms), 
                                           sample_rate=sample_rate)
    for subquery_name, subquery_result in subquery_results.items():
        sampling_query = sampling_query.replace(subquery_name, subquery_result)
    start = time.time()
    job_id = str(int(start*100))
    log_file = f"logs/{query.name}-{job_id}.log"
    setup_logging(log_file=log_file)

    results_df = execute_query(conn, sampling_query, dbms)
    runtime = time.time() - start

    close_connection(conn, dbms)
    result_file = f"results/{query.name}-sample_only-{dbms}-{job_id}.csv"
    dump_results(result_file=result_file, results_df=results_df)

    logging.info(f"sample only execution time: {runtime}")
    logging.info(f"sample only result:\n{results_df}")

    return results_df, {"sample_only_runtime": runtime}