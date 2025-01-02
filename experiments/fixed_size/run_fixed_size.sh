tpch_db_path=$1

for run in {1..10}; do
    for qid in 1 5 6 7 8 9 12 14 19; do
        for process_mode in order_by_random_rows tsm_system_rows exact; do
            pg_ctl -D $tpch_db_path stop
            sudo sync
            echo 3 | sudo tee /proc/sys/vm/drop_caches
            sleep 2
            pg_ctl -D $tpch_db_path start
            python experiments/fixed_size/run_fixed_size.py \
                --qid $qid \
                --process_mode $process_mode \
                --db_config_file db_configs/postgres_tpch.yml
        done
    done
done