db_path=$1

# postgres
for run in {1..10}; do
    for qid in 1 2 3 4 8 21 30; do
        pg_ctl -D $db_path stop
        sudo sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        sleep 2
        pg_ctl -D $db_path start
        python evaluate.py \
            --benchmark clickbench \
            --qid $qid \
            --dbms postgres \
            --db_config_file db_configs/postgres_clickbench.yml \
            --process_mode aqp
    done
done

# duckdb
for run in {1..10}; do
    for qid in 1 2 3 4 8 21 30; do
        sudo sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        sleep 2
        python evaluate.py \
            --benchmark clickbench \
            --qid $qid \
            --dbms duckdb \
            --db_config_file db_configs/duckdb_clickbench.yml \
            --process_mode aqp
    done
done

# sql server
for run in {1..10}; do
    for qid in 1 2 3 4 8 21 30; do
        python evaluate.py \
            --benchmark clickbench \
            --qid $qid \
            --dbms sqlserver \
            --db_config_file db_configs/sqlserver_clickbench.yml \
            --process_mode aqp
    done
done
