import pilotdb
import time

query = """SELECT
    sum(l_extendedprice * l_discount) as revenue
FROM
    lineitem
WHERE
    l_shipdate >= date '1994-01-01'
    AND l_shipdate < date '1994-01-01' + interval '1' year
    AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
    AND l_quantity < 24;

"""

db_config = {
    "dbms": "duckdb",  # or duckdb, sqlserver
    "username": "",
    "path": "/mydata/tpch-sf100.db",
    "host": "",
    "port": "",
    "password": "",
}
conn = pilotdb.connect("duckdb", db_config)
start = time.time()
result = pilotdb.run(
    conn,
    query=query,
    error=0.05,
    probability=0.05,  # the failure probability
)
pilotdb_runtime = time.time() - start
print(result)
pilotdb.close(conn)

print(f"PilotDB runtime: {pilotdb_runtime:.4f} seconds")
