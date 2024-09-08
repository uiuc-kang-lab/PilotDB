for sample_rate in 0.01 0.02 0.03 0.04 0.05 0.06 0.07 0.08 0.09 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0 10.0 20.0 30.0 40.0 50.0 60.0 70.0 80.0 90.0 100.0
do
    for mode in "page" "row" "shuffle"
    do
        pg_ctl -D /mydata/tpch/tpch/ps stop
        sudo sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        pg_ctl -D /mydata/tpch/tpch/ps start
        sleep 2
        python run.py --user teng77 --mode $mode --sample_rate $sample_rate
    done
done
