from q3 import exp_query as q3_exp_query
from q6 import exp_query as q6_exp_query
from q7 import exp_query as q7_exp_query
from q8 import exp_query as q8_exp_query
from q12 import exp_query as q12_exp_query
from q18 import exp_query as q18_exp_query
from q23a import exp_query as q23a_exp_query
from q28 import exp_query as q28_exp_query
from q30 import exp_query as q30_exp_query
from q40 import exp_query as q40_exp_query
from q48 import exp_query as q48_exp_query
from q55 import exp_query as q55_exp_query
from q70 import exp_query as q70_exp_query
from q86 import exp_query as q86_exp_query
from q91 import exp_query as q91_exp_query
from q92 import exp_query as q92_exp_query
from q96 import exp_query as q96_exp_query
from pilotdb.db_driver.postgres_utils import connect_to_db, execute_query, close_connection
import multiprocessing
import time
import os

queue = multiprocessing.Queue()

def run(query, db_name, user_name):
    conn = connect_to_db(db=db_name, user=user_name)
    results = execute_query(conn, query)
    close_connection(conn)
    queue.put(results)

def start_db(db_path):
    os.system(f"pg_ctl -D {db_path} -l logfile start")

def stop_db(db_path):
    os.system(f"pg_ctl -D {db_path} stop")

# experiment start
queries = {
    # "q3": q3_exp_query,
    # "q6": q6_exp_query,
    # "q7": q7_exp_query,
    # "q8": q8_exp_query,
    # "q12": q12_exp_query,
    # "q18": q18_exp_query,
    "q23a": q23a_exp_query,
    "q28": q28_exp_query,
    "q30": q30_exp_query,
    "q40": q40_exp_query,
    "q48": q48_exp_query,
    "q55": q55_exp_query,
    "q70": q70_exp_query,
    "q86": q86_exp_query,
    "q91": q91_exp_query,
    "q92": q92_exp_query,
    "q96": q96_exp_query
}

stop_db("/mydata/tpcds/ps")
start_db("/mydata/tpcds/ps")

for query_nb, query in queries.items():
    try:
        print(f"Running query {query_nb}")
        process = multiprocessing.Process(target=run, args=(query, "tpcds1t", "teng77"))
        process.start()
        process.join(timeout=3600)
        if process.is_alive():
            print(f"Query {query_nb} took too long to execute")
            process.terminate()
            process.join()
        elif process.exitcode == 0:
            print(f"Query {query_nb} finished")
            results = queue.get()
            results.to_csv(f"{query_nb}_1tb.csv")
    except Exception as e:
        print(f"1TB Query {query_nb} failed due to {e}")

stop_db("/mydata/tpcds_500gb_postgres")
start_db("/mydata/tpcds_500gb_postgres")

for query_nb, query in queries.items():
    try:
        print(f"Running query {query_nb}")
        process = multiprocessing.Process(target=run, args=(query, "tpcds", "yxx404"))
        process.start()
        process.join(timeout=3600)
        if process.is_alive():
            print(f"Query {query_nb} took too long to execute")
            process.terminate()
            process.join()
        else:
            print(f"Query {query_nb} finished")
            results = queue.get()
            results.to_csv(f"{query_nb}_1tb.csv")
    except Exception as e:
        print(f"500GB Query {query_nb} failed due to {e}")




