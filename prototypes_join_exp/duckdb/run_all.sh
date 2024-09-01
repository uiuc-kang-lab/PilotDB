for i in {1..10}
do
    for query_id in 5 7 8 9 12 14 19
    do
        python run_prototype.py --query_id $query_id
    done
done

