from sample_queries import query_5, query_7, query_8, query_12, query_14, query_19
import psycopg2
import requests
import sys
import os
import time

def explain_query(query_id: int, offset: int, query_t: str):
    with psycopg2.connect(dbname="tpch1t", user="teng77", host="localhost", port=5432) as conn:
        with open(f"q{query_id}_sample_rates.csv") as f:
            line = f.readlines()[offset]
            sample_rates = [float(x) for x in line.strip().split(",")]
            for i, sample_rate in enumerate(sample_rates):
                if sample_rate >= 100:
                    query_t = query_t.replace("{p" + str(i+1) + "}", "")
                else:
                    query_t = query_t.replace("{p" + str(i+1) + "}", f"TABLESAMPLE SYSTEM({sample_rate})")
        with conn.cursor() as cur:
            cur.execute("EXPLAIN" + query_t)
            cost = cur.fetchall()[0][0].split("..")[-1].split(" ")[0]
        with open(f"q{query_id}_cost.csv", "a+") as f:
            sample_rates_str = ",".join([str(x) for x in sample_rates])
            f.write(f"{sample_rates_str},{cost}\n")
    return

def run_query(query_id: int, offset: int, query_t: str):
    with psycopg2.connect(dbname="tpch1t", user="teng77", host="localhost", port=5432) as conn:
        with open(f"q{query_id}_sample_rates.csv") as f:
            line = f.readlines()[offset]
            sample_rates = [float(x) for x in line.strip().split(",")]
            for i, sample_rate in enumerate(sample_rates):
                if sample_rate >= 100:
                    query_t = query_t.replace("{p" + str(i+1) + "}", "")
                else:
                    query_t = query_t.replace("{p" + str(i+1) + "}", f"TABLESAMPLE SYSTEM({sample_rate})")
        timeout = 1000
        if os.path.exists(f"q{query_id}_runtime.csv"):
            with open(f"q{query_id}_runtime.csv") as f:
                for line in f:
                    runtime = float(line.strip().split(",")[-2])
                    if runtime < timeout:
                        timeout = int(runtime) - 1

        timeout *= 2
        is_timeout = False  
        with conn.cursor() as cur:
            start = time.time()
            try:
                cur.execute("SET statement_timeout = " + str(timeout*1000) + ";" + query_t)
            except psycopg2.errors.QueryCanceled:
                is_timeout = True
            end = time.time()
            runtime = end - start
        with open(f"q{query_id}_runtime.csv", "a+") as f:
            sample_rates_str = ",".join([str(x) for x in sample_rates])
            f.write(f"{sample_rates_str},{runtime},{is_timeout}\n")

if __name__ == "__main__":
    query_id = int(sys.argv[1])
    offset = int(sys.argv[2])
    if query_id == 5:
        query_t = query_5
    elif query_id == 7:
        query_t = query_7
    elif query_id == 8:
        query_t = query_8
    elif query_id == 12:
        query_t = query_12
    elif query_id == 14:
        query_t = query_14
    elif query_id == 19:
        query_t = query_19
    else:
        print("Invalid query id")
        sys.exit(1)
    run_query(query_id, offset, query_t)
