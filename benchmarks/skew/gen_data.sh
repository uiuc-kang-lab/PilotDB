for child in {1..10}
do
    screen -S order$child -d -m
    screen -S order$child -X stuff $"conda activate pilotdb\npython gen_data.py --scale 100 --skew 2.5 --parallel 10 --child $child --table order\n"
done

for child in {1..80}
do
    screen -S lineitem$child -d -m
    screen -S lineitem$child -X stuff $"conda activate pilotdb\npython gen_data.py --scale 100 --skew 2.5 --parallel 80 --child $child --table lineitem\n"
done