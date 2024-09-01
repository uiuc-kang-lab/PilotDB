
for i in {1..4}
do
    for query in query_12.sql query_14.sql query_19.sql query_1.sql query_5.sql query_6.sql query_7.sql query_8.sql query_9.sql
    do
        echo $query
        psql -d tpch1t -U teng77 -f $query -o $query.$i.out
    done
done