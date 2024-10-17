for run in {1..10}
do
    for sel in {1..10}
    do
        python run_selectivity.py --selectivity $sel
    done
done