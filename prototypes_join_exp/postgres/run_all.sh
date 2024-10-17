for offset in {3..52}
do
    pg_ctl -D /mydata/tpch/tpch/ps/ stop
    sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches;
    pg_ctl -D /mydata/tpch/tpch/ps/ start
    python run_sampling.py 5 $offset
done

for offset in {1..27}
do
    sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches;
    python run_sampling.py 12 $offset
done

for offset in {1..47}
do
    sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches;
    python run_sampling.py 8 $offset
done

for offset in {1..24}
do
    sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches;
    python run_sampling.py 14 $offset
done

for offset in {1..50}
do
    sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches;
    python run_sampling.py 7 $offset
done

for offset in {1..13}
do
    sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches;
    python run_sampling.py 19 $offset
done

