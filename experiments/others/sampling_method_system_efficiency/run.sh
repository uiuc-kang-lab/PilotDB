username=$1
password=$2
dbname=$3
db_path=$4

for i in {1..5}
do
    pg_ctl -D $db_path stop
    sudo sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches
    pg_ctl -D $db_path start
    sleep 2
    python run.py --user $username --password $password --dbname $dbname --mode exact

    pg_ctl -D $db_path stop
    sudo sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches
    pg_ctl -D $db_path start
    sleep 2
    python run.py --user $username --password $password --dbname $dbname --mode page

    pg_ctl -D $db_path stop
    sudo sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches
    pg_ctl -D $db_path start
    sleep 2
    python run.py --user $username --password $password --dbname $dbname --mode row

    pg_ctl -D $db_path stop
    sudo sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches
    pg_ctl -D $db_path start
    sleep 2
    python run.py --user $username --password $password --dbname $dbname --mode shuffle
done