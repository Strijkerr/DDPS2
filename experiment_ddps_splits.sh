#!/bin/sh
#nodes=${1}
print(${1})

# counter=0
# > Times/ddps_100000000.txt
# while [ $counter -le 9 ]
# do
# python3 create_data.py --seed $counter --sequence_length 100000000
# python3 main.py --nodes node115,node116,node117 -- input sequence.npy --partitions 1 --splits 5 --copies 1
# ((counter++))
# done