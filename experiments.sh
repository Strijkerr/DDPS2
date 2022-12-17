#!/bin/sh
counter=0
> single_system.txt
while [ $counter -le 9 ]
do
python3 create_data.py --seed $counter --sequence_length 1000000 | grep "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> single_system.txt
((counter++))
done