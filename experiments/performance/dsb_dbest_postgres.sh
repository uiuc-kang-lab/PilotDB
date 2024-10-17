#!/bin/bash

for run in {1..5}; do
    for qid in {1..97}; do
        pg_ctl -D /mydata/dsb_postgres stop
        bash clean_cache.sh
        pg_ctl -D /mydata/dsb_postgres start
        python run_pilotdb.py \
            --benchmark dbest \
            --qid agg$qid \
            --pilot_sample_rate 0.05 \
            --dbms postgres \
            --db_config_file db_configs/postgres_dsb.yml \
            --process_mode aqp
        
    done
    for qid in {1..30}; do
        pg_ctl -D /mydata/dsb_postgres stop
        bash clean_cache.sh
        pg_ctl -D /mydata/dsb_postgres start
        python run_pilotdb.py \
            --benchmark dbest \
            --qid groupby$qid \
            --dbms postgres \
            --db_config_file db_configs/postgres_dsb.yml \
            --process_mode aqp
    done
    for qid in {1..42}; do
        pg_ctl -D /mydata/dsb_postgres stop
        bash clean_cache.sh
        pg_ctl -D /mydata/dsb_postgres start
        python run_pilotdb.py \
            --benchmark dbest \
            --qid join$qid \
            --dbms postgres \
            --db_config_file db_configs/postgres_dsb.yml \
            --process_mode aqp
    done
done
