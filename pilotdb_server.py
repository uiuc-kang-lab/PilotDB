# a pilotdb flask server

from flask import Flask, request, jsonify
import pilotdb
from pilotdb.db_driver.driver import execute_query
import os
import time

app = Flask(__name__)

@app.route('/test_postgres_connection', methods=['GET'])
def test_postgres_connection():
    password = os.getenv("POSTGRES_PASSWORD", "PilotDB123")
    username = os.getenv("POSTGRES_USER", "pilotdb")

    db_config = {
        "dbname": "postgres",
        "username": username,
        "host": "postgres",
        "port": 5432,
        "password": password
    }

    try:
        conn = pilotdb.connect(
            "postgres",
            db_config
        )

        pilotdb.close(conn)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    return jsonify({"status": "success", "message": "Postgres connection successful"}), 200

@app.route('/test_sqlserver_connection', methods=['GET'])
def test_sqlserver_connection():
    password = os.getenv("SQLSERVER_SA_PASSWORD", "PilotDB123")
    db_config = {
        "dbname": "msdb",
        "username": "sa",
        "host": "sqlserver",
        "password": password
    }
    try:
        conn = pilotdb.connect(
            "sqlserver",
            db_config
        )
        pilotdb.close(conn)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    return jsonify({"status": "success", "message": "SQL Server connection successful"}), 200

@app.route('/test_duckdb_connection', methods=['GET'])
def test_duckdb_connection():
    db_config = {
        "path": "test.duckdb"
    }
    try:
        conn = pilotdb.connect(
            "duckdb",
            db_config
        )
        pilotdb.close(conn)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    return jsonify({"status": "success", "message": "DuckDB connection successful"}), 200

@app.route('/run_aqp_query', methods=['POST'])
def run_aqp_query():
    data = request.json
    assert data, "Request body must be JSON"
    
    query = data.get('query')
    db_config = data.get('db_config')
    error = data.get('error', 0.05)
    probability = data.get('probability', 0.05)
    database = data.get('database', 'postgres')

    if not query or not db_config:
        return jsonify({"status": "error", "message": "Query and db_config are required"}), 400

    try:
        conn = pilotdb.connect(
            database,
            db_config
        )
        start = time.time()
        result = pilotdb.run(
            conn,
            query,
            error,
            probability)
        runtime = time.time() - start
        pilotdb.close(conn)
        return jsonify({"status": "success", "runtime": runtime, "result": result.to_dict()}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    
@app.route('/run_exact_query', methods=['POST'])
def run_exact_query():
    data = request.json
    assert data, "Request body must be JSON"
    
    query = data.get('query')
    db_config = data.get('db_config')
    database = data.get('database', 'postgres')

    if not query or not db_config:
        return jsonify({"status": "error", "message": "Query and db_config are required"}), 400

    try:
        conn = pilotdb.connect(
            database,
            db_config
        )
        start = time.time()
        result = execute_query(conn['conn'], query, database)
        runtime = time.time() - start
        pilotdb.close(conn)
        return jsonify({"status": "success", "runtime": runtime, "result": result.to_dict()}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500