for i in {0..2}
do
    # psql -d skew -U yuxuan18 -c "\copy skew_1_5 FROM '/mydata/skew_data/z_1.5_$i.csv' WITH CSV HEADER;"

    # psql -d skew -U yuxuan18 -c "\copy skew_2 FROM '/mydata/skew_data/z_2_$i.csv' WITH CSV HEADER;"

    # psql -d skew -U yuxuan18 -c "\copy skew_2_5 FROM '/mydata/skew_data/z_2.5_$i.csv' WITH CSV HEADER;"

    # psql -d skew -U yuxuan18 -c "\copy skew_3 FROM '/mydata/skew_data/z_3_$i.csv' WITH CSV HEADER;"

    psql -d skew -U yuxuan18 -c "\copy skew_3_5 FROM '/mydata/skew_data/z_3.5_$i.csv' WITH CSV HEADER;"

    psql -d skew -U yuxuan18 -c "\copy skew_4 FROM '/mydata/skew_data/z_4_$i.csv' WITH CSV HEADER;"
done