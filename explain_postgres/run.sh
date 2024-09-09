for i in {1..5}
do
    for query_file in $(ls clickhouse/*.sql)
    do
        pg_ctl -D /mydata/postgres_clickbench_ssb stop
        sudo sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        pg_ctl -D /mydata/postgres_clickbench_ssb start
        query=$(cat $query_file)
        echo "EXPLAIN ANALYZE $query" | psql -d clickbench -U yuxuan18 -o $query_file.$i.out
    done
done

for i in {1..5}
do
    for query_file in $(ls ssb/*.sql)
    do
        pg_ctl -D /mydata/postgres_clickbench_ssb stop
        sudo sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        pg_ctl -D /mydata/postgres_clickbench_ssb start
        query=$(cat $query_file)
        echo "EXPLAIN ANALYZE $query" | psql -d ssb -U yuxuan18 -o $query_file.$i.out
    done
done