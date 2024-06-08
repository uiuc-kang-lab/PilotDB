for run in {1..5}
do
    for sel in 1 2 3 4 5
    do
        python run_selectivity.py --selectivity $sel
    done
done