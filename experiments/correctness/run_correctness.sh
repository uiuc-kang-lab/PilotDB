#!/bin/bash
for run in {1..100}
do
    for query in "tpch-6-m"
    do
        for error in 0.0125 0.015 0.0175 0.02 0.0225
        do
            pg_ctl -D /mydata/ps/ps stop
            sudo sync
            echo 3 | sudo tee /proc/sys/vm/drop_caches
            pg_ctl -D /mydata/ps/ps start
            sleep 2
            python run_exp.py \
                --query $query \
                --error $error
        done
        # for fp in 0.001 0.005 0.01 0.05 0.1
        # do
        #     pg_ctl -D /mydata/ps/ps stop
        #     sudo sync
        #     echo 3 | sudo tee /proc/sys/vm/drop_caches
        #     pg_ctl -D /mydata/ps/ps start
        #     sleep 2
        #     python run_exp.py \
        #         --query $query \
        #         --failure_probability $fp \
        #         --error 0.01
        # done
    done
done
