from query_templates import gt_query_5, gt_query_7, gt_query_8, gt_query_9, gt_query_12, gt_query_14, gt_query_19
import duckdb
import requests
import sys

def run_query(query_str: str, save_file: str):
    with duckdb.connect("/mydata/tpch_1t.duckdb") as conn:
        copy_statement = f"COPY ({query_str}) TO '{save_file}' WITH CSV HEADER"
        conn.sql(copy_statement)
    return

if __name__ == "__main__":
    qid = int(sys.argv[1])

    if qid == 5:
        run_query(gt_query_5, "/mydata/gt_query_5_duckdb.csv")
    elif qid == 7:
        run_query(gt_query_7, "/mydata/gt_query_7_duckdb.csv")
    elif qid == 8:
        run_query(gt_query_8, "/mydata/gt_query_8_duckdb.csv")
    elif qid == 9:
        run_query(gt_query_9, "/mydata/gt_query_9_duckdb.csv")
    elif qid == 12:
        run_query(gt_query_12, "/mydata/gt_query_12_duckdb.csv")
    elif qid == 14:
        run_query(gt_query_14, "/mydata/gt_query_14_duckdb.csv")
    elif qid == 19:
        run_query(gt_query_19, "/mydata/gt_query_19_duckdb.csv")
