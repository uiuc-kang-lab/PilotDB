import pilotdb

db_config = {
    "dbms": "postgres",  # or duckdb, sqlserver
    "username": "yuxuan18",
    "dbname": "postgres",
    "host": "localhost",
    "port": 5432,
    "password": "",
}
conn = pilotdb.connect("postgres", db_config)
result = pilotdb.run(
    conn,
    query="SELECT COUNT(*) FROM order_products",
    error=0.05,
    probability=0.05,  # the failure probability
)
print(result)
pilotdb.close(conn)
