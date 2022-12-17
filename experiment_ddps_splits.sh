#!/bin/sh
nodes=${1}
read -ra node_list <<< "$nodes"; unset IFS
# Different copies
> Times/ddps_partitions_1_splits_5_copies_1.txt
> Times/ddps_partitions_1_splits_5_copies_2.txt

# Different partitions
> Times/ddps_partitions_2_splits_5_copies_1.txt
> Times/ddps_partitions_3_splits_5_copies_1.txt
counter=0
while [ $counter -le 9 ]
do
python3 create_data.py --seed $counter --sequence_length 10000000 --measure_performance False > /dev/null

# Different copies
python3 main.py --nodes $node_list --input sequence.npy --partitions 1 --splits 5 --copies 1 | grep -s "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/ddps_partitions_1_splits_5_copies_1.txt
python3 main.py --nodes $node_list --input sequence.npy --partitions 1 --splits 5 --copies 2 | grep -s "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/ddps_partitions_1_splits_5_copies_2.txt

# Different partitions
python3 main.py --nodes $node_list --input sequence.npy --partitions 2 --splits 5 --copies 1 | grep -s "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/ddps_partitions_2_splits_5_copies_1.txt
python3 main.py --nodes $node_list --input sequence.npy --partitions 3 --splits 5 --copies 1 | grep -s "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/ddps_partitions_3_splits_5_copies_1.txt
((counter++))
done

# Different copies
python3 Times/experiments_ddps.py "Times/ddps_partitions_1_splits_5_copies_1.txt"
python3 Times/experiments_ddps.py "Times/ddps_partitions_1_splits_5_copies_2.txt"

# Different partitions
python3 Times/experiments_ddps.py "Times/ddps_partitions_2_splits_5_copies_1.txt"
python3 Times/experiments_ddps.py "Times/ddps_partitions_3_splits_5_copies_1.txt"