#!/bin/bash

# Define an array of qid values
qid_values=(1 2 3 5 6 7 9 10 11 12) #1 2 3 4 5 6 7 8 9 10 11 12 13) #2 3 4 8 13 15 16 17 18 19 21 28 30 31 32 33 34 35 36 37 38 39 40 41 42 43) 

# Outer loop for qid values
for qid in "${qid_values[@]}"
do
    # Inner loop for running five times
    for run in {1..1}
    do
        python run_pilotdb.py \
            --benchmark tpch \
            --qid $qid \
            --pilot_sample_rate 0.05 \
            --dbms postgres \
            --db_config_file /users/teng77/PilotDB/db_configs/postgres_tpch.yml \
            --process_mode aqp
    done

    python run_pilotdb.py \
        --benchmark tpch \
        --qid $qid \
        --dbms postgres \
        --db_config_file /users/teng77/PilotDB/db_configs/postgres_tpch.yml \
        --process_mode exact
done
