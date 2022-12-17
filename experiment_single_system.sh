#!/bin/sh
counter=0
> Times/single_system_100000.txt
> Times/single_system_1000000.txt
> Times/single_system_10000000.txt
#> Times/single_system_100000000.txt
while [ $counter -le 9 ]
do
python3 create_data.py --seed $counter --sequence_length 100000 | grep "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/single_system_100000.txt
python3 create_data.py --seed $counter --sequence_length 1000000 | grep "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/single_system_1000000.txt
python3 create_data.py --seed $counter --sequence_length 10000000 | grep "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/single_system_10000000.txt
#python3 create_data.py --seed $counter --sequence_length 100000000 | grep "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/single_system_100000000.txt
((counter++))
done

python3 Times/experiments_single_system.py