for run in {1..5}
do
    for rate in 0.05 0.1
    do
        python run_pilot_rate.py --pilot_sample_rate $rate
    done
done