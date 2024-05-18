#!/bin/bash

# Define an array of qid values
qid_values=(26 27 40 45 62 99 50 8 48 96 7 13 15 18)

# Outer loop for qid values
for qid in "${qid_values[@]}"
do
    # Inner loop for running five times
    for run in {1..5}
    do
        pg_ctl -D /mydata/ps/ps stop
        sudo sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        pg_ctl -D /mydata/ps/ps start
        sleep 2
        python run_pilotdb.py \
            --benchmark tpcds \
            --qid $qid \
            --pilot_sample_rate 0.05 \
            --dbms postgres \
            --db_config_file /users/teng77/PilotDB/db_configs/postgres_tpcds.yml \
            --process_mode aqp
    done

    pg_ctl -D /mydata/ps/ps stop
    sudo sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches
    pg_ctl -D /mydata/ps/ps start
    sleep 2
    python run_pilotdb.py \
        --benchmark tpcds \
        --qid $qid \
        --dbms postgres \
        --db_config_file /users/teng77/PilotDB/db_configs/postgres_tpcds.yml \
        --process_mode exact
done
