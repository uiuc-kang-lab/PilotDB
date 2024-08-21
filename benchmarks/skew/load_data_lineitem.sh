for child in {1..80}
do
    psql -d postgres -c "\copy lineitem FROM /mydata/skew/lineitem_$child.csv DELIMITER ',' CSV"
done
