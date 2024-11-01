import sys
import duckdb
import time
from sqlglot import transpile
import argparse
import yaml
from pilotdb.pilot_engine.rewriter.pilot import Pilot_Rewriter
from pilotdb.pilot_engine.rewriter.sampling import Sampling_Rewriter
from pilotdb.db_driver.driver import *
from pilotdb.pilot_engine.utils import aggregate_error_to_page_error
from pilotdb.pilot_engine.error_bounds import estimate_final_rate
from pilotdb.utils.utils import get_largest_sample_rate


def read_meta_data(conn, verbose: bool=False):
    table_cols = {}
    table_sizes = []
    if verbose:
        print("Reading metadata...")
    start = time.time()
    tables = conn.execute("SHOW TABLES").fetchall()
    tables = [table[0] for table in tables]
    for table in tables:
        size = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchall()[0][0]
        columns = conn.execute(f"SELECT * FROM {table} LIMIT 1").description
        columns = [column[0] for column in columns]
        table_cols[table] = columns
        table_sizes.append((table, size))
    table_sizes = sorted(table_sizes, key=lambda x: x[1], reverse=True)
    table_sizes = [table[0] for table in table_sizes]
    end = time.time()
    if verbose:
        print(f"Runtime: {end-start:.2f} seconds")
    
    return table_cols, table_sizes

def cold_start_duckdb(dbfile):
    conn = duckdb.connect(dbfile)
    version = conn.execute("SELECT version()").fetchall()[0][0]
    print(f"Running PilotDB 1.0 (DuckDB {version})")
    return conn

def process_subqueries(dbms, conn, pq):
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
                if isinstance(subquery_result[0], str) or isinstance(
                    subquery_result[0], pd.Timestamp
                ):
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

def execute_original_query(conn, query):
    try:
        result = conn.sql(query)
        return result
    except Exception as e:
        return str(e)
    
def highlight_print(stuff: str):
    print("\033[91m {}\033[00m".format(stuff))

def execute_approximate_query(conn, query: str, table_cols, table_size, dbms, error, confidence,
                              pilot_sampling_rate: float=0.05, verbose: bool=False):
    pq = Pilot_Rewriter(table_cols, table_size, dbms)
    sq = Sampling_Rewriter(table_cols, table_size, dbms)
    pilot_query = pq.rewrite(query) + ";"
    sampling_query = sq.rewrite(query) + ";"
    sampling_clause = get_sampling_clause(pilot_sampling_rate, dbms)
    pilot_query = pilot_query.format(sampling_method=sampling_clause)
    subquery_results = process_subqueries(dbms, conn, pq)
    for subquery_name, subquery_result in subquery_results.items():
        pilot_query = pilot_query.replace(subquery_name, subquery_result)
    
    if verbose:
        highlight_print("Executing a pilot query:")
        print(transpile(pilot_query, read=dbms, pretty=True)[0].replace("PERCENT", "%"))
    pilot_results = execute_query(conn, pilot_query, dbms)
    
    if verbose:
        highlight_print("Solving sampling rate ...")
    page_errors = aggregate_error_to_page_error(
            pq.result_mapping_list, required_error=error
        )
    final_sample_rate = estimate_final_rate(
            failure_prob=1-confidence,
            pilot_results=pilot_results,
            page_errors=page_errors,
            group_cols=pq.group_cols,
            pilot_rate=pilot_sampling_rate / 100,
            limit=pq.limit_value,
        )
    
    if final_sample_rate == -1 or final_sample_rate * 100 > get_largest_sample_rate(dbms):
        if verbose:
            # print in red color
            highlight_print("Sampling rate is too high, running original query ...")
        return execute_original_query(conn, query)
    else:
        sampling_query = sampling_query.format(
            sampling_method=sampling_clause, sample_rate=pilot_sampling_rate / 100
        )
        if verbose:
            highlight_print("Executing the final query")
            print(transpile(sampling_query, read=dbms, pretty=True)[0].replace("PERCENT", "%"))
        for subquery_name, subquery_result in subquery_results.items():
            sampling_query = sampling_query.replace(subquery_name, subquery_result)
        result = execute_original_query(conn, sampling_query)
        return result
    
def parse_query(query: str):
    query_lower_case = query.lower()
    if query_lower_case.strip().split(" ")[0] != "select":
        return "ORIGINAL", None, None, None
    elif "avg" not in query_lower_case and "sum" not in query_lower_case and "count" not in query_lower_case:
        return "ORIGINAL", None, None, None
    else:
        if "error within " in query_lower_case and "confidence " in query_lower_case:
            try:
                start_idx = query_lower_case.index("error within ") + len("error within ")
                end_idx = query.index("%", start_idx)
                error = float(query[start_idx:end_idx])/100
                
                start_idx = query_lower_case.index("confidence ") + len("confidence ")
                end_idx = query.index("%", start_idx)
                confidence = float(query[start_idx:end_idx])/100

                query_end = min(query_lower_case.index("error within "), query_lower_case.index("confidence "))
                query = query[:query_end]
                return "APPROXIMATE", query, error, confidence
            except:
                return "Syntax Error: Invalid error bound or confidence", None, None, None
        else:
            return "ORIGINAL", None, None, None


def execute_query_duckdb(conn, query: str, table_cols, table_size, verbose):
    if query.strip().split(" ")[0].lower() != "select":
        return execute_original_query(conn, query)
    elif "avg" not in query.lower() and "sum" not in query.lower() and "count" not in query.lower():
        return execute_original_query(conn, query)
    else:
        mode, processed_query, error, confidence = parse_query(query) # type: ignore
        if mode == "ORIGINAL":
            return execute_original_query(conn, query)
        elif mode == "APPROXIMATE":
            return execute_approximate_query(conn, processed_query, table_cols, table_size, "duckdb", error, confidence, verbose=verbose) # type: ignore
        else:
            return mode

def run_duckdb(dbfile, verbose):
    conn = cold_start_duckdb(dbfile)
    table_cols, table_sizes = read_meta_data(conn, verbose)
    use_timer = False
    while True:
        sys.stdout.write("D> ")
        sys.stdout.flush()
        session_input_lines = []
        while True:
            session_input_line = input()
            session_input_lines.append(session_input_line)
            if session_input_line[-1] == ";":
                break
        session_input = " ".join(session_input_lines)
        if session_input.strip() == "exit;":
            break
        if session_input.strip() == "":
            continue
        if session_input.strip()[:-1].strip() == ".timer on":
            use_timer = True
            continue
        if session_input.strip()[:-1].strip() == ".timer off":
            use_timer = False
            continue
        if use_timer:
            start = time.time()
        result = execute_query_duckdb(conn, session_input, table_cols, table_sizes, verbose)
        print(result)
        if use_timer:
            end = time.time()
            highlight_print(f"Runtime: {end-start:.2f} seconds")
    conn.close()

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--database", type=str, help="Database name")
    arg_parser.add_argument("--config", type=str, help="Database config")
    arg_parser.add_argument("--verbose", type=str, default="False", help="Verbose mode")
    args = arg_parser.parse_args()
    args.verbose = eval(args.verbose)
    
    yaml_config = yaml.load(open(args.config, "r"), Loader=yaml.FullLoader)
    
    run_duckdb(yaml_config["dbname"], args.verbose)
    