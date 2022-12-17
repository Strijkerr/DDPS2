#!/bin/sh
counter=0
length = 1000000
> single_system_1000000.txt
> single_system_10000000.txt
> single_system_100000000.txt
> single_system_1000000000.txt
while [ $counter -le 9 ]
do
python3 create_data.py --seed $counter --sequence_length 1000000 | grep "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> single_system_1000000.txt
python3 create_data.py --seed $counter --sequence_length 10000000 | grep "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> single_system_10000000.txt
python3 create_data.py --seed $counter --sequence_length 100000000 | grep "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> single_system_100000000.txt
python3 create_data.py --seed $counter --sequence_length 1000000000 | grep "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> single_system_1000000000.txt
((counter++))
done