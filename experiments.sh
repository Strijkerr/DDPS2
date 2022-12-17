#!/bin/sh
counter=0
length = 1000000
> single_system{"$length"}.txt
while [ $counter -le 9 ]
do
python3 create_data.py --seed $counter --sequence_length $length | grep "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> single_system{"$length"}.txt
((counter++))
done