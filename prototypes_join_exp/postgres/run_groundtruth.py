from query_templates import gt_query_5, gt_query_7, gt_query_8, gt_query_9, gt_query_12, gt_query_14, gt_query_19
import psycopg2
import requests
import sys

def run_query(query_str: str, save_file: str):
    with psycopg2.connect(dbname="tpch1t", user="teng77", host="localhost", port=5432) as conn:
        copy_statement = f"COPY ({query_str}) TO '{save_file}' WITH CSV HEADER"
        with conn.cursor() as cur:
            cur.execute(copy_statement)
            res = cur.fetchall()
            print(res)
    return

if __name__ == "__main__":
    qid = int(sys.argv[1])

    if qid == 5:
        run_query(gt_query_5, "/mydata/gt_query_5.csv")
    elif qid == 7:
        run_query(gt_query_7, "/mydata/gt_query_7.csv")
    elif qid == 8:
        run_query(gt_query_8, "/mydata/gt_query_8.csv")
    elif qid == 9:
        run_query(gt_query_9, "/mydata/gt_query_9.csv")
    elif qid == 12:
        run_query(gt_query_12, "/mydata/gt_query_12.csv")
    elif qid == 14:
        run_query(gt_query_14, "/mydata/gt_query_14.csv")
    elif qid == 19:
        run_query(gt_query_19, "/mydata/gt_query_19.csv")
