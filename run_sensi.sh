# search constraints: delta_1 + delta_2 + fp = 0.05, delta_1 >= 0.001, delta_2 >= 0.001, fp >= 0.001
# iterate over any combination of delta_1, delta_2, and fp
# run the sensitivity analysis for each combination
tpch_db_path=/mydata/ps
for delta_1 in $(seq 0.001 0.001 0.048)
do
    delta_2_max=$(echo "0.05 - $delta_1 - 0.001" | bc)
    for delta_2 in $(seq 0.001 0.001 $delta_2_max)
    do
        fp=$(echo "0.05 - $delta_1 - $delta_2" | bc)
        pg_ctl -D $tpch_db_path stop
        sudo sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        sleep 2
        pg_ctl -D $tpch_db_path start
        python evaluate.py \
            --benchmark tpch \
            --qid 6 \
            --pilot_sample_rate 0.05 \
            --dbms postgres \
            --db_config_file db_configs/postgres_tpch.yml \
            --process_mode aqp \
            --error 0.01 \
            --failure_probability 0.05 \
            --delta_1 $delta_1 \
            --delta_2 $delta_2
    done
done
