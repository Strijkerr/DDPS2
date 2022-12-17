#!/bin/sh
nodes=${1}
read -ra node_list <<< "$nodes"; unset IFS

counter=0
while [ $counter -le 9 ]
do
python3 create_data.py --seed $counter --sequence_length 100000000 --measure_performance False > /dev/null
python3 main.py --nodes $node_list -- input sequence.npy --partitions 1 --splits 5 --copies 1 | grep "time" | cut -d ":" -f 2 | cut -d " " -f 2
((counter++))
done