import psycopg2
import argparse
import time
import json

parser = argparse.ArgumentParser()
parser.add_argument("--dbname", type=str, default="postgres")
parser.add_argument("--user", type=str)
parser.add_argument("--password", type=str, default="")
parser.add_argument("--mode", type=str)
parser.add_argument("--sample_rate", type=float)
args = parser.parse_args()

conn = psycopg2.connect(dbname=args.dbname, user=args.user, password=args.password)
cur = conn.cursor()

if args.mode == "page":
    with open("page-level-dsb.sql") as f:
        query = f.readlines()[0]
elif args.mode == "row":
    with open("row-level-dsb.sql") as f:
        query = f.readlines()[0]
elif args.mode == "shuffle":
    args.sample_rate /= 100
    with open("data-shuffle-dsb.sql") as f:
        query = f.readlines()[0]
elif args.mode == "exact":
    with open("no-sample-dsb.sql") as f:
        query = f.readlines()[0]

# DEBUG
# query = "set statement_timeout to 1000;" + query

query = query.format(sample_rate=args.sample_rate)
start = time.time()
cur.execute(query)
result = float(cur.fetchall()[0][0])
end = time.time()

running_results = {
    "mode": args.mode,
    "sample_rate": args.sample_rate,
    "avg": result,
    "time": end - start
}

with open("results-dsb.jsonl", "a+") as f:
    f.write(json.dumps(running_results) + "\n")