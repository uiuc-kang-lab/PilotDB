screen -dmS bs05 conda activate pilotdb 
screen -dmS bs05 python test_query_bs.py 0 5
screen -dmS bs510 conda activate pilotdb 
screen -dmS bs510 python test_query_bs.py 5 10
screen -dmS bs1015 conda activate pilotdb 
screen -dmS bs1015 python test_query_bs.py 10 15
screen -dmS bs1520 conda activate pilotdb 
screen -dmS bs1520 python test_query_bs.py 15 20