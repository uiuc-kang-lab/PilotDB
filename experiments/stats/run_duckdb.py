import duckdb
import pandas as pd
import json

con = duckdb.connect(database='/mydata/tpch_1t.duckdb', read_only=False)

exact_query = """
SELECT AVG( l_extendedprice ) as avg, STDDEV(l_extendedprice) as std, count(*) as cnt
FROM lineitem, orders
WHERE l_orderkey = o_orderkey AND l_comment LIKE '%special%'
"""

exact_res = con.execute(exact_query).fetch_df().to_dict()
with open("exact_results.jsonl", "w") as f:
    f.write(json.dumps(exact_res) + "\n")

aqp_query = """
SELECT AVG( l_extendedprice ) as avg, STDDEV(l_extendedprice) as std, count(*) as cnt
FROM lineitem TABLESAMPLE SYSTEM(1%), orders TABLESAMPLE SYSTEM(5%) 
WHERE l_orderkey = o_orderkey AND l_comment LIKE '%special%'
"""
for _ in range(1000):
    res = con.execute(aqp_query).fetch_df().to_dict()
    with open("aqp_results.jsonl", "a+") as f:
        f.write(json.dumps(res) + "\n")

con.close()
