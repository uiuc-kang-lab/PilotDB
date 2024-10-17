for child in {1..10}
do
    psql -d postgres -c "\copy orders FROM /mydata/skew/order_$child.csv DELIMITER ',' CSV"
done
