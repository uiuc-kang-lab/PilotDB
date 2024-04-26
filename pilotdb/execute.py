from pilotdb.pilot_engine.rewriter.pilot import Pilot_Rewriter
from pilotdb.pilot_engine.rewriter.sampling import Sampling_Rewriter
from pilotdb.pilot_engine.commons import *
from pilotdb.db_engine.query import connect_to_db, execute_query, close_connection, get_sampling_clause
from pilotdb.pilot_engine.utils import aggregate_error_to_page_error
from pilotdb.pilot_engine.error_bounds import estimate_final_rate
from pilotdb.utils import setup_logging, dump_results

import json
import logging
import time

def execute_aqp(benchmark: str, qid: str, pilot_sample_rate: float=0.05, 
                dbms: str=POSTGRES, db_config_file: str="db_configs/postgres.yml"):
    log_file = f"logs/{benchmark}-{qid}-{time.time}.log"
    setup_logging(log_file=log_file)

    # read query and meta info
    query_file = f"benchmarks/{benchmark}/query_{qid}.sql"
    meta_file = f"benchmarks/{benchmark}/meta.json"

    with open(query_file, "r") as f:
        original_query = f.read()

    with open(meta_file, "r") as f:
        meta = json.load(f)

    pq = Pilot_Rewriter(meta["table_cols"], meta["table_size"], dbms)
    sq = Sampling_Rewriter(meta["table_cols"], meta["table_size"], dbms)

    pilot_query = pq.rewrite(original_query)
    sampling_query = sq.rewrite(original_query)

    logging.info(f"original query:\n{original_query}")
    logging.info(f"sampling query:\n{sampling_query}")
    logging.info(f"pilot query:\n{pilot_query}")
    logging.info(f"column mapping: {pq.result_mapping_list}")
    logging.info(f"group cols: {pq.group_cols}")
    logging.info(f"subquery dict: {pq.subquery_dict}")
    logging.info(f"res2pageid: {pq.res_2_page_id}")

    # construct and execute pilot sampling query
    sampling_clause = get_sampling_clause(pilot_sample_rate, dbms)
    pilot_query = pilot_query.format(sampling_method=sampling_clause)
    start = time.time()
    conn = connect_to_db(dbms, db_config_file)
    pilot_results = execute_query(conn, pilot_query, dbms)
    pilot_execution_time = time.time() - start
    logging.info(f"pilot query executing time: {pilot_execution_time}")

    # parse the results of pilot query
    page_errors = aggregate_error_to_page_error(pq.result_mapping_list)
    logging.info(f"converted page errors: {page_errors}")
    final_sample_rate = estimate_final_rate(failure_prob=0.05, pilot_results=pilot_results, page_errors=page_errors,
                                            group_cols=pq.group_cols, pilot_rate=pilot_sample_rate)
    sample_rate_solving_time = time.time() - start - pilot_execution_time
    logging.info(f"sample rate solving time: {sample_rate_solving_time}")

    if final_sample_rate == -1:
        results_df = execute_query(conn, original_query, dbms)
        logging.info(f"fail to solve sample rate, fall back to original queries")
    elif dbms != DUCKDB and final_sample_rate > 0.05:
        results_df = execute_query(conn, original_query, dbms)
        logging.info(f"too big sample rate {final_sample_rate*100}, fall back to original queries")
    elif final_sample_rate*100 > pilot_sample_rate:
        final_sample_rate = round(final_sample_rate*100, 2)
        logging.info(f"final sample rate: {final_sample_rate}")
        sampling_clause = get_sampling_clause(final_sample_rate, dbms)
        sampling_query = sampling_query.format(sample_method=sampling_clause, sample_rate=final_sample_rate)
        results_df = execute_query(conn, sampling_query, dbms)
    sampling_execution_time = time.time() - start - pilot_execution_time - sample_rate_solving_time
    logging.info(f"sampling execution time: {sampling_execution_time}")

    total_runtime = time.time() - start
    close_connection(conn, dbms)
    
    runtime = {
        "pilot_execution_time": pilot_execution_time,
        "sample_rate_solving_time": sample_rate_solving_time,
        "sampling_execution_time": sampling_execution_time,
        "total_runtime": total_runtime
    }

    result_file = f"results/{benchmark}-{qid}-pilot-{pilot_sample_rate}-{dbms}.jsonl"
    dump_results(result_file=result_file, results_df=results_df)

    return results_df, runtime

    

def execute_exact(benchmark: str, qid: str, dbms: str=POSTGRES, db_config_file: str="db_configs/postgres.yml"):
    # read query
    query_file = f"benchmarks/{benchmark}/query_{qid}.sql"
    with open(query_file, "r") as f:
        original_query = f.read()

    conn = connect_to_db(dbms, db_config_file)
    start = time.time()
    results_df = execute_query(conn, original_query, dbms)
    runtime = time.time() - start

    close_connection(conn, dbms)
    result_file = f"results/{benchmark}-{qid}-exact.jsonl"
    dump_results(result_file=result_file, results_df=results_df)

    return results_df, {"exact_runtime": runtime}