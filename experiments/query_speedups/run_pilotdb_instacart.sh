db_path=$1

# postgres
for run in {1..10}; do
    for qid in 1 6 7 8 9 11 12 14; do
        pg_ctl -D $db_path stop
        sudo sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        sleep 2
        pg_ctl -D $db_path start
        python evaluate.py \
            --benchmark instacart \
            --qid $qid \
            --dbms postgres \
            --db_config_file db_configs/postgres_instacart.yml \
            --process_mode aqp
    done
done

# duckdb
for run in {1..10}; do
    for qid in 1 6 7 8 9 11 12 14; do
        sudo sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        sleep 2
        python evaluate.py \
            --benchmark instacart \
            --qid $qid \
            --dbms duckdb \
            --db_config_file db_configs/duckdb_instacart.yml \
            --process_mode aqp
    done
done

# sql server
for run in {1..10}; do
    for qid in 1 6 7 8 9 11 12 14; do
        python evaluate.py \
            --benchmark instacart \
            --qid $qid \
            --dbms sqlserver \
            --db_config_file db_configs/sqlserver_instacart.yml \
            --process_mode aqp
    done
done
