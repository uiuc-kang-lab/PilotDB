#!/bin/bash

# Define an array of qid values
qid_values=(1)

# Outer loop for qid values
for qid in "${qid_values[@]}"
do
    # Inner loop for running five times
    for run in {1..5}
    do
        python run_pilotdb.py \
            --benchmark tpch \
            --qid $qid \
            --pilot_sample_rate 0.05 \
            --dbms duckdb \
            --db_config_file /users/teng77/PilotDB/db_configs/duckdb_t p ch.yml \
            --process_mode aqp
    done
    python run_pilotdb.py \
        --benchmark tpch \
        --qid $qid \
        --dbms duckdb \
        --db_config_file /users/teng77/PilotDB/db_configs/duckdb_tpch.yml \
        --process_mode exact
    curl -d "query $qid successful" ntfy.sh/tpchps
done