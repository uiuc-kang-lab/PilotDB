from pilotdb.query import *
from pilotdb.utils.path import *
from pilotdb.utils.timer import Timer
from pilotdb.db_driver.driver import *
from pilotdb.pilot_engine.commons import *
from pilotdb.pilot_engine.rewriter.pilot import Pilot_Rewriter
from pilotdb.pilot_engine.error_bounds import estimate_final_rate, estimate_final_rate_oracle_tpch1
from pilotdb.pilot_engine.rewriter.sampling import Sampling_Rewriter
from pilotdb.pilot_engine.utils import aggregate_error_to_page_error
from pilotdb.utils.utils import setup_logging, dump_results, get_largest_sample_rate

import json
import time
import logging
import pandas as pd
from typing import Dict
from sqlglot import transpile

def execute_oracle_aqp(query: Query, db_config: dict, pilot_sample_rate: float=100):
    # prepare the query and db
    dbms = db_config["dbms"]
    conn = connect_to_db(dbms, db_config)
    
    pq = Pilot_Rewriter(query.table_cols, query.table_size, dbms)
    sq = Sampling_Rewriter(query.table_cols, query.table_size, dbms)
    pilot_query = pq.rewrite(query.query) + ";"
    sampling_query = sq.rewrite(query.query) + ";"
    sampling_clause = ''
    pilot_query = pilot_query.format(sampling_method=sampling_clause)

    # start execution
    timer = Timer()
    job_id = str(int(timer.start()*100))
    setup_logging(log_file=get_log_file_path("logs", query.name, job_id))
    log_query_info(query, dbms)
    pq.log_info()
    if dbms == DUCKDB and not pq.is_rewritable:
        final_sample_rate = 100
        sampling_query = query.query
        subquery_results = {}
    elif dbms == SQLSERVER and directly_run_exact(conn, query.query, pilot_query, dbms, pq.largest_table):
        final_sample_rate = 100
        logging.info(f"retrieving query plan time: {timer.check('query_plan_time')}")
        subquery_results = {}
    else:
        if query.name == "tpch-1" and (dbms == POSTGRES or dbms == SQLSERVER):
            if dbms == POSTGRES:
                pilot_query = """
SELECT
    AVG(r2) AS avg_1,
    STDDEV(r2) AS std_1,
    AVG(r3) AS avg_2,
    STDDEV(r3) AS std_2,
    AVG(r4) AS avg_3,
    STDDEV(r4) AS std_3,
    AVG(r5) AS avg_4,
    STDDEV(r5) AS std_4,
    AVG(r8) AS avg_5,
    STDDEV(r8) AS std_5,
    AVG(r9) AS avg_6,
    STDDEV(r9) AS std_6,
    COUNT(*) AS n_page
FROM (
    SELECT 
        l_returnflag AS r0, 
        l_linestatus AS r1, 
        SUM(l_quantity) AS r2, 
        SUM(l_extendedprice) AS r3, 
        SUM(l_extendedprice * (1 - l_discount)) AS r4, 
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS r5, 
        SUM(l_discount) AS r8, 
        COUNT(*) AS r9, 
        CAST((CAST(CAST(lineitem.ctid AS TEXT) AS point))[0] AS INT) AS page_id_0 
    FROM lineitem
    WHERE l_shipdate <= CAST('1998-12-01' AS DATE) - INTERVAL '90' DAY 
    GROUP BY l_returnflag, l_linestatus, page_id_0
)
GROUP BY r0, r1;
"""
            else:
                pilot_query = """
SELECT
    AVG(r2) AS avg_1,
    STDEV(r2) AS std_1,
    AVG(r3) AS avg_2,
    STDEV(r3) AS std_2,
    AVG(r4) AS avg_3,
    STDEV(r4) AS std_3,
    AVG(r5) AS avg_4,
    STDEV(r5) AS std_4,
    AVG(r8) AS avg_5,
    STDEV(r8) AS std_5,
    AVG(r9) AS avg_6,
    STDEV(r9) AS std_6,
    COUNT(*) AS n_page
FROM (
    SELECT 
        l_returnflag AS r0, 
        l_linestatus AS r1, 
        SUM(l_quantity) AS r2, 
        SUM(l_extendedprice) AS r3, 
        SUM(l_extendedprice * (1 - l_discount)) AS r4, 
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS r5, 
        SUM(l_discount) AS r8, 
        COUNT_BIG(*) AS r9, 
        CAST(CAST(SUBSTRING(lineitem.%%physloc%%, 6, 1)
                    + SUBSTRING(lineitem.%%physloc%%, 5, 1) AS int) AS VARCHAR) 
                    + '||' + CAST(CAST(SUBSTRING(lineitem.%%physloc%%, 4, 1) 
                    + SUBSTRING(lineitem.%%physloc%%, 3, 1) + SUBSTRING(lineitem.%%physloc%%, 2, 1) 
                    + SUBSTRING(lineitem.%%physloc%%, 1, 1) AS int) AS VARCHAR) AS page_id_0 
    FROM lineitem
    WHERE l_shipdate <= DATEADD(day, -90, '1998-12-01')
    GROUP BY l_returnflag, l_linestatus, 
        CAST(CAST(SUBSTRING(lineitem.%%physloc%%, 6, 1)
                    + SUBSTRING(lineitem.%%physloc%%, 5, 1) AS int) AS VARCHAR) 
                    + '||' + CAST(CAST(SUBSTRING(lineitem.%%physloc%%, 4, 1) 
                    + SUBSTRING(lineitem.%%physloc%%, 3, 1) + SUBSTRING(lineitem.%%physloc%%, 2, 1) 
                    + SUBSTRING(lineitem.%%physloc%%, 1, 1) AS int) AS VARCHAR)

) T
GROUP BY r0, r1;
"""
            logging.info(f"pilot query:\n{pilot_query}")
            pilot_results = execute_query(conn, pilot_query, dbms)
            logging.info(f"pilot query executing time: {timer.check('pilot_query_execution')}")
            final_sample_rate = estimate_final_rate_oracle_tpch1(pilot_results)
            
        else:
            # execute subqueries
            subquery_results = process_subqueries(dbms, conn, pq)

            # execute pilot query
            for subquery_name, subquery_result in subquery_results.items():
                pilot_query = pilot_query.replace(subquery_name, subquery_result)
            
            logging.info(f"pilot query:\n{pilot_query}")

            pilot_results = execute_query(conn, pilot_query, dbms)
            logging.info(f"pilot query executing time: {timer.check('pilot_query_execution')}")

            # parse the results of pilot query
            page_errors = aggregate_error_to_page_error(pq.result_mapping_list)
            logging.info(f"converted page errors: {page_errors}")
            final_sample_rate = estimate_final_rate(failure_prob=0.05, pilot_results=pilot_results, page_errors=page_errors,
                                                    group_cols=pq.group_cols, pilot_rate=pilot_sample_rate/100, limit=pq.limit_value)
        logging.info(f"sample rate solving time: {timer.check('sampling_rate_solving')}")

        if final_sample_rate == -1:
            final_sample_rate = 100
            logging.info(f"fail to solve sample rate, fall back to original queries")
        elif final_sample_rate*100 > get_largest_sample_rate(dbms):
            logging.info(f"too big sample rate {final_sample_rate*100}, fall back to original queries")
            final_sample_rate = 100
    timer.stop()
    close_connection(conn, dbms)

    with open("all_results.jsonl", "a+") as f:
        result = {"query": query.name, "dbms": dbms, "pilot_sample_rate": pilot_sample_rate, "final_sample_rate": final_sample_rate,
                  "runtime": timer.get_records()}
        f.write(json.dumps(result) + "\n")
        
    for i in range(5):
        execute_sample_only(query, final_sample_rate, db_config)
    



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
                  "results_file": get_result_file_path("./results", query.name, job_id, "exact", dbms)}
        f.write(json.dumps(result) + "\n")

    return results_df, timer.get_records()

def execute_sample_only(query: Query, sample_rate: float, db_config: dict):
    dbms = db_config["dbms"]
    conn = connect_to_db(dbms, db_config)
    sq = Sampling_Rewriter(query.table_cols, query.table_size, dbms)
    sampling_query = sq.rewrite(query.query) + ";"

    subquery_results = process_subqueries(dbms, conn, sq)
    if sample_rate == 100:
        sampling_query = sampling_query.format(sampling_method="", sample_rate="1")
    else:
        sampling_query = sampling_query.format(sampling_method=get_sampling_clause(sample_rate*100, dbms), 
                                            sample_rate=sample_rate)
    for subquery_name, subquery_result in subquery_results.items():
        sampling_query = sampling_query.replace(subquery_name, subquery_result)
    start = time.time()
    job_id = str(int(start*100))

    logging.info(f"sampling query:\n{sampling_query}")
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