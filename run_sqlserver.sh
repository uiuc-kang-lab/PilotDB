#!/bin/bash

# Define an array of qid values
qid_values=(48 45 62 69 92 99) # 8 13 15 18 26 27 40 45 48 62 99)

# Outer loop for qid values
for qid in "${qid_values[@]}"
do
    # Inner loop for running five times
    for run in {1..5}
    do
        python run_pilotdb.py \
            --benchmark tpcds \
            --qid $qid \
            --pilot_sample_rate 0.05 \
            --dbms sqlserver \
            --db_config_file /users/teng77/PilotDB/db_configs/sqlserver_tpcds.yml \
            --process_mode aqp
    done
    python run_pilotdb.py \
        --benchmark tpcds \
        --qid $qid \
        --dbms sqlserver \
        --db_config_file /users/teng77/PilotDB/db_configs/sqlserver_tpcds.yml \
        --process_mode exact
    curl -d "query $qid successful" ntfy.sh/tpchps
done