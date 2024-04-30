from pilotdb.pilot_engine.rewriter.pilot import Pilot_Rewriter
from pilotdb.pilot_engine.rewriter.sampling import Sampling_Rewriter
from pilotdb.pilot_engine.commons import *
from pilotdb.db_driver.query import connect_to_db, execute_query, close_connection, get_sampling_clause
from pilotdb.pilot_engine.utils import aggregate_error_to_page_error
from pilotdb.pilot_engine.error_bounds import estimate_final_rate
from pilotdb.utils import setup_logging, dump_results, get_largest_sample_rate
from pilotdb.query import Query

import time
from typing import Dict
import logging

def execute_aqp(query: Query, db_config: dict, pilot_sample_rate: float=0.05):
    # prepare the query and db
    dbms = db_config["dbms"]
    conn = connect_to_db(dbms, db_config)
    
    pq = Pilot_Rewriter(query.table_cols, query.table_size, dbms)
    sq = Sampling_Rewriter(query.table_cols, query.table_size, dbms)
    pilot_query = pq.rewrite(query.query) + ";"
    sampling_query = sq.rewrite(query.query) + ";"
    sampling_clause = get_sampling_clause(pilot_sample_rate, dbms)
    pilot_query = pilot_query.format(sampling_method=sampling_clause)

    # start execution
    start = time.time()
    job_id = str(int(start*100))
    log_file = f"logs/{query.name}-{job_id}.log"
    setup_logging(log_file=log_file)
    logging.info(f"start approximately processing query {query.name} with pilot sample rate {pilot_sample_rate} on {dbms}")
    logging.info(f"original query:\n{query.query}")
    logging.info(f"column mapping: {pq.result_mapping_list}")
    logging.info(f"group cols: {pq.group_cols}")
    logging.info(f"subquery dict: {pq.subquery_dict}")
    logging.info(f"res2pageid: {pq.res_2_page_id}")
    logging.info(f"subqueries in WHERE and HAVING: {pq.subquery_dict}")

    # execute subqueries
    subquery_results = process_subqueries(dbms, conn, pq)

    # execute pilot query
    for subquery_name, subquery_result in subquery_results.items():
        pilot_query = pilot_query.replace(subquery_name, subquery_result)
    logging.info(f"pilot query:\n{pilot_query}")

    pilot_results = execute_query(conn, pilot_query, dbms)
    pilot_results_file = f"results/{query.name}-pilot-{pilot_sample_rate}-{dbms}-{job_id}.csv"
    dump_results(result_file=pilot_results_file, results_df=pilot_results)
    pilot_execution_time = time.time() - start
    logging.info(f"pilot query executing time: {pilot_execution_time}")

    # parse the results of pilot query
    page_errors = aggregate_error_to_page_error(pq.result_mapping_list)
    logging.info(f"converted page errors: {page_errors}")
    final_sample_rate = estimate_final_rate(failure_prob=0.05, pilot_results=pilot_results, page_errors=page_errors,
                                            group_cols=pq.group_cols, pilot_rate=pilot_sample_rate/100)
    sample_rate_solving_time = time.time() - start - pilot_execution_time
    logging.info(f"sample rate solving time: {sample_rate_solving_time}")

    # # ========== debug begin
    # final_sample_rate = 0.01
    # # ========== debug end


    if final_sample_rate == -1:
        final_sample_rate = 1
        logging.info(f"fail to solve sample rate, fall back to original queries")
    elif final_sample_rate*100 > get_largest_sample_rate(dbms):
        final_sample_rate = 1
        logging.info(f"too big sample rate {final_sample_rate*100}, fall back to original queries")
    
    if final_sample_rate == 1:
        sampling_query = sampling_query.format(sampling_method="", sample_rate="1")
        for subquery_name, subquery_result in subquery_results.items():
            sampling_query = sampling_query.replace(subquery_name, subquery_result)
        logging.info(f"sampling query:\n{sampling_query}")
        results_df = execute_query(conn, sampling_query, dbms)
    elif final_sample_rate*100 > pilot_sample_rate:
        final_sample_rate = round(final_sample_rate*100, 2)
        logging.info(f"final sample rate: {final_sample_rate}")
        sampling_clause = get_sampling_clause(final_sample_rate, dbms)
        sampling_query = sampling_query.format(sampling_method=sampling_clause, sample_rate=final_sample_rate/100)
        for subquery_name, subquery_result in subquery_results.items():
            sampling_query = sampling_query.replace(subquery_name, subquery_result)
        logging.info(f"sampling query:\n{sampling_query}")
        results_df = execute_query(conn, sampling_query, dbms)
    else:
        logging.info(f"final sample rate: {final_sample_rate}, pilot sampling is large enough")
        results_df = pilot_results
    sampling_execution_time = time.time() - start - pilot_execution_time - sample_rate_solving_time
    logging.info(f"sampling execution time: {sampling_execution_time}")
    logging.info(f"aqp result:\n{results_df}")

    total_runtime = time.time() - start
    close_connection(conn, dbms)
    
    runtime = {
        "pilot_execution_time": pilot_execution_time,
        "sample_rate_solving_time": sample_rate_solving_time,
        "sampling_execution_time": sampling_execution_time,
        "total_runtime": total_runtime
    }

    result_file = f"results/{query.name}-aqp-{pilot_sample_rate}-{dbms}.jsonl"
    dump_results(result_file=result_file, results_df=results_df)

    return results_df, runtime

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
                if isinstance(subquery_result[0], str):
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
    start = time.time()
    job_id = str(int(start*100))
    log_file = f"logs/{query.name}-{job_id}.log"
    setup_logging(log_file=log_file)
    results_df = execute_query(conn, query.query, dbms)
    runtime = time.time() - start

    close_connection(conn, dbms)
    result_file = f"results/{query.name}-exact-{dbms}-{job_id}.csv"
    dump_results(result_file=result_file, results_df=results_df)

    logging.info(f"exact execution time: {runtime}")
    logging.info(f"exact result:\n{results_df}")

    return results_df, {"exact_runtime": runtime}

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