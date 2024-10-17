for run in {1..10}
do
    for rate in 0.05 0.1 0.5 1 5 10
    do
        python run_pilot_rate.py --pilot_sample_rate $rate
    done
done