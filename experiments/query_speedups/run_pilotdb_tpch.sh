db_path=$1

# postgres
for run in {1..10}; do
    for qid in 1 5 6 7 8 9 12 14 19; do
        pg_ctl -D $db_path stop
        sudo sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        sleep 2
        pg_ctl -D $db_path start
        python evaluate.py \
            --benchmark tpch \
            --qid $qid \
            --dbms postgres \
            --db_config_file db_configs/postgres_tpch.yml \
            --process_mode aqp
    done
done

# duckdb
for run in {1..10}; do
    for qid in 1 5 6 7 8 9 12 14 19; do
        sudo sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        sleep 2
        python evaluate.py \
            --benchmark tpch \
            --qid $qid \
            --dbms duckdb \
            --db_config_file db_configs/duckdb_tpch.yml \
            --process_mode aqp
    done
done

# sql server
for run in {1..10}; do
    for qid in 1 5 6 7 8 9 12 14 19; do
        python evaluate.py \
            --benchmark tpch \
            --qid $qid \
            --dbms sqlserver \
            --db_config_file db_configs/sqlserver_tpch.yml \
            --process_mode aqp
    done
done
