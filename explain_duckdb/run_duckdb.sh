enable_profiling="PRAGMA enable_profiling = 'json';"

for i in {2..5}; do

    for query_file in $(ls tpch-*.sql); do
        output_file="PRAGMA profile_output = '$query_file.$i.json';"
        query=$(cat $query_file)
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        echo "$enable_profiling $output_file $query" | ./duckdb /mydata/tpch_1t.duckdb
    done

    for query_file in $(ls clickhouse-*.sql); do
        output_file="PRAGMA profile_output = '$query_file.$i.json';"
        query=$(cat $query_file)
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        echo "$enable_profiling $output_file $query" | ./duckdb /mydata/clickbench_condensed.duckdb
    done

    for query_file in $(ls ssb-*.sql); do
        output_file="PRAGMA profile_output = '$query_file.$i.json';"
        query=$(cat $query_file)
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        echo "$enable_profiling $output_file $query" | ./duckdb /mydata/ssb-1000.duckdb
    done

done