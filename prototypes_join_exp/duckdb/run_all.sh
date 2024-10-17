for i in {1..2}
do
    for query_id in 5 7 8 9 12 14 19
    do
        python run_prototype.py --query_id $query_id --mode single_table
        python run_prototype.py --query_id $query_id --mode multi_table
        python run_prototype.py --query_id $query_id --mode exact
    done
done