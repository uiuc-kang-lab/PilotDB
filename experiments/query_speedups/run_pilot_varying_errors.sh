tpch_db_path=$1
ssb_db_path=$2
clickbench_db_path=$3
instacart_db_path=$4
dsb_db_path=$5

for error in 0.01 0.025 0.05 0.075 0.1; do
    # tpch
    for run in {1..10}; do
        for qid in 1 5 6 7 8 9 12 14 19; do
            pg_ctl -D $tpch_db_path stop
            sudo sync
            echo 3 | sudo tee /proc/sys/vm/drop_caches
            sleep 2
            pg_ctl -D $tpch_db_path start
            python evaluate.py \
                --benchmark tpch \
                --qid $qid \
                --dbms postgres \
                --db_config_file db_configs/postgres_tpch.yml \
                --process_mode aqp \
                --error $error
        done
    done

    # ssb
    for run in {1..10}; do
        for qid in 1 2 3 5 6 7 9 10 11 12; do
            pg_ctl -D $ssb_db_path stop
            sudo sync
            echo 3 | sudo tee /proc/sys/vm/drop_caches
            sleep 2
            pg_ctl -D $ssb_db_path start
            python evaluate.py \
                --benchmark ssb \
                --qid $qid \
                --dbms postgres \
                --db_config_file db_configs/postgres_ssb.yml \
                --process_mode aqp \
                --error $error
        done
    done

    # clickbench
    for run in {1..10}; do
        for qid in 1 2 3 4 8 21 30; do
            pg_ctl -D $clickbench_db_path stop
            sudo sync
            echo 3 | sudo tee /proc/sys/vm/drop_caches
            sleep 2
            pg_ctl -D $clickbench_db_path start
            python evaluate.py \
                --benchmark clickbench \
                --qid $qid \
                --dbms postgres \
                --db_config_file db_configs/postgres_clickbench.yml \
                --process_mode aqp \
                --error $error
        done
    done

    # instacart
    for run in {1..10}; do
        for qid in 1 6 7 8 9 11 12 14; do
            pg_ctl -D $instacart_db_path stop
            sudo sync
            echo 3 | sudo tee /proc/sys/vm/drop_caches
            sleep 2
            pg_ctl -D $instacart_db_path start
            python evaluate.py \
                --benchmark instacart \
                --qid $qid \
                --dbms postgres \
                --db_config_file db_configs/postgres_instacart.yml \
                --process_mode aqp \
                --error $error
        done
    done

    # dsb
    for run in {1..20}; do
        for qid in {1..97}; do
            pg_ctl -D $dsb_db_path stop
            sudo sync
            echo 3 | sudo tee /proc/sys/vm/drop_caches
            sleep 2
            pg_ctl -D $dsb_db_path start
            python evaluate.py \
                --benchmark dbest \
                --qid agg$qid \
                --dbms postgres \
                --db_config_file db_configs/postgres_dsb.yml \
                --process_mode aqp \
                --error $error
        done
        for qid in {1..30}; do
            pg_ctl -D $dsb_db_path stop
            sudo sync
            echo 3 | sudo tee /proc/sys/vm/drop_caches
            sleep 2
            pg_ctl -D $dsb_db_path start
            python evaluate.py \
                --benchmark dbest \
                --qid groupby$qid \
                --dbms postgres \
                --db_config_file db_configs/postgres_dsb.yml \
                --process_mode aqp \
                --error $error
        done
        for qid in {1..42}; do
            pg_ctl -D $dsb_db_path stop
            sudo sync
            echo 3 | sudo tee /proc/sys/vm/drop_caches
            sleep 2
            pg_ctl -D $dsb_db_path start
            python evaluate.py \
                --benchmark dbest \
                --qid join$qid \
                --dbms postgres \
                --db_config_file db_configs/postgres_dsb.yml \
                --process_mode aqp \
                --error $error
        done
    done
done