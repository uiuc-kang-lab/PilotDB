import pandas as pd

from pilotdb.pilot_engine.utils import aggregate_error_to_page_error
from pilotdb.pilot_engine.error_bounds import estimate_final_rate
from pilotdb.db_engine.query import connect_to_db, execute_query, close_connection
from pilotdb.pilot_engine.query_base import Query
import time
from typing import List, Dict


def get_queries(qid: str):
    if qid == "tpch1":
        from pilotdb.pilot_engine.manual_query.tpch1 import query
        page_id_table="lineitem"
    elif qid == "tpch4":
        from pilotdb.pilot_engine.manual_query.tpch4 import query
        page_id_table="orders"
    elif qid == "tpch6":
        from pilotdb.pilot_engine.manual_query.tpch6 import query
        page_id_table="lineitem"
    elif qid == "tpch7":
        from pilotdb.pilot_engine.manual_query.tpch7 import query
        page_id_table="lineitem"
    elif qid == "tpch9":
        from pilotdb.pilot_engine.manual_query.tpch9 import query
        page_id_table="lineitem"
    elif qid == "tpch12":
        from pilotdb.pilot_engine.manual_query.tpch12 import query
        page_id_table="lineitem"
    elif qid == "tpch19":
        from pilotdb.pilot_engine.manual_query.tpch19 import query
        page_id_table="lineitem"
    else:
        raise NotImplemented(f"query {qid} is not implemented")
    return query, page_id_table

def run_aqp(dbms: str, query: Query, page_id_table: str, pilot_rate: float):
    if dbms == "duckdb":
        conn = connect_to_db("duckdb", path="/mydata/tpch_1t.duckdb")
        pilot_query = query.pilot_query.format(
            page_id=f"floor({page_id_table}.rowid/2048)",
            sample=f"TABLESAMPLE SYSTEM({pilot_rate}%)")

    elif dbms == "postgres":
        conn = connect_to_db("postgres", db="tpch_1t", user="yuxuan")
        pilot_query = query.pilot_query.format(
            page_id=f"({page_id_table}.ctid::text::point)[0]::int",
            sample=f"TABLESAMPLE SYSTEM ({pilot_rate})")
    else:
        raise NotImplemented(f"DBMS {dbms} is not supported")
    
    start = time.time()
    results_df = execute_query(conn, pilot_query, dbms)
    pilot_query_time = time.time() - start
    print(f"pilot query time {pilot_query_time}")
    page_errors = aggregate_error_to_page_error(query.column_mapping, 
                                                    query.page_size_col)
    final_sample_rate = estimate_final_rate(0.05, results_df, page_errors, 
                                            query.group_cols, query.page_size_col, 
                                            query.page_id_col, 
                                            pilot_rate=pilot_rate/100)
    pilot_solving_time = time.time() - start - pilot_query_time
    print(f"pilot solving time {pilot_solving_time}")

    if final_sample_rate == -1:
        results_df = execute_query(conn, query.original_query, dbms)
    else:
        final_sample_rate = round(final_sample_rate * 100, 2)
        print(f"final sample rate {final_sample_rate}")
        
        if dbms == "duckdb":
            final_sample_query = query.final_sample_query.format(sample=f"TABLESAMPLE SYSTEM({final_sample_rate}%)")
        elif dbms == "postgres":
            final_sample_query = query.final_sample_query.format(sample=f"TABLESAMPLE SYSTEM ({final_sample_rate})")

        results_df = execute_query(conn, final_sample_query, dbms)
    final_query_time = time.time() - start - pilot_query_time - pilot_solving_time
    print(f"final query time {final_query_time}")

    total_runtime = time.time() - start
    close_connection(conn, dbms)

    runtime = {
        "pilot_query_time": pilot_query_time,
        "pilot_solving_time": pilot_solving_time,
        "final_query_time": final_query_time,
        "total_runtime": total_runtime
    }

    return results_df, runtime

def run_exact(dbms: str, query: Query):
    if dbms == "duckdb":
        conn = connect_to_db("duckdb", path="/mydata/tpch_1t.duckdb")

    elif dbms == "postgres":
        conn = connect_to_db("postgres", db="tpch_1t", user="yuxuan")

    else:
        raise NotImplementedError(f"DBMS {dbms} is not supported")

    start = time.time()
    results_df = execute_query(conn, query.original_query, dbms)
    exact_runtime = time.time() - start
    close_connection(conn, dbms)

    runtime = {
        "exact_runtime": exact_runtime
    }

    return results_df, runtime

def aggregate_runtimes(runtimes: List[Dict]):
    agg_runtime = runtimes[0]
    for runtime in runtimes[1:]:
        for key in runtime:
            agg_runtime[key] += runtime[key]
    
    for key in agg_runtime:
        agg_runtime[key] /= len(runtimes)
    
    return agg_runtime

if __name__ == "__main__":
    dbms = "duckdb"
    pilot_rate = 0.05

    for qid in ["tpch1", "tpch4", "tpch6", "tpch7", "tpch9", "tpch12", "tpch19"]:
        print(qid)
        try:
            query, page_id_table = get_queries(qid)
            exact_runtimes = []
            aqp_runtimes = []
            for _ in range(10):
                results_df, exact_runtime = run_exact(dbms, query)
                results_df, aqp_runtime = run_aqp(dbms, query, page_id_table, pilot_rate)
                exact_runtimes.append(exact_runtime)
                aqp_runtimes.append(aqp_runtime)

            exact_runtime = aggregate_runtimes(exact_runtimes)
            aqp_runtime = aggregate_runtimes(aqp_runtimes)
            print(exact_runtime)
            print(aqp_runtime)

        except Exception as e:
            print(f"fail to execute query {qid} due to {e}")