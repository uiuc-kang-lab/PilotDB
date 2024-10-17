db_path=$1

# postgres
for run in {1..20}; do
    for qid in {1..97}; do
        pg_ctl -D $db_path stop
        sudo sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        sleep 2
        pg_ctl -D $db_path start
        python evaluate.py \
            --benchmark dbest \
            --qid agg$qid \
            --dbms postgres \
            --db_config_file db_configs/postgres_dsb.yml \
            --process_mode aqp
    done
    for qid in {1..30}; do
        pg_ctl -D $db_path stop
        sudo sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        sleep 2
        pg_ctl -D $db_path start
        python evaluate.py \
            --benchmark dbest \
            --qid groupby$qid \
            --dbms postgres \
            --db_config_file db_configs/postgres_dsb.yml \
            --process_mode aqp
    done
    for qid in {1..42}; do
        pg_ctl -D $db_path stop
        sudo sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        sleep 2
        pg_ctl -D $db_path start
        python evaluate.py \
            --benchmark dbest \
            --qid join$qid \
            --dbms postgres \
            --db_config_file db_configs/postgres_dsb.yml \
            --process_mode aqp
    done
done
