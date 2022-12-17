#!/bin/sh
nodes=${1}
IFS=',' read -ra node_list <<< "$nodes"; unset IFS
master=${node_list[0]}
worker=${node_list[@]:1}
echo "master is "$master
echo "worker is "$worker

# counter=0
# > Times/ddps_100000000.txt
# while [ $counter -le 9 ]
# do
# python3 create_data.py --seed $counter --sequence_length 100000000
# python3 main.py --nodes node115,node116,node117 -- input sequence.npy --partitions 1 --splits 5 --copies 1
# ((counter++))
# done