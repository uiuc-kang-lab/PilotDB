for query_id in 5 14 19 # 7 8 9 12 
do
    for i in {1..10}
    do
        python run_prototype.py --query_id $query_id
    done
done

