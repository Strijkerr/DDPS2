#!/bin/sh
counter=0
while [ $counter -le 9 ]
do
python3 create_data.py --seed $counter --sequence_length 1000000 | grep "time" | cut -c 20-35 #&> filename.txt
((counter++))
done