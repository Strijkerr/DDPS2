#!/bin/sh
counter=0
local -i int
int=1000000
> single_system_"$length".txt
while [ $counter -le 9 ]
do
python3 create_data.py --seed $counter --sequence_length "$int" | grep "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> single_system_"$length".txt
((counter++))
done