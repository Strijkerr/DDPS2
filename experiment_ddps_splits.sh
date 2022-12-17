#!/bin/sh
nodes=${1}
read -ra node_list <<< "$nodes"; unset IFS

# Base
> Times/ddps_partitions_1_splits_5_copies_1.txt

# Different partitions & splits
# > Times/ddps_partitions_1_splits_10_copies_1.txt
# > Times/ddps_partitions_2_splits_5_copies_1.txt
# > Times/ddps_partitions_2_splits_10_copies_1.txt

# Different copies
# > Times/ddps_partitions_1_splits_5_copies_2.txt
# > Times/ddps_partitions_1_splits_10_copies_2.txt
# > Times/ddps_partitions_2_splits_5_copies_2.txt
# > Times/ddps_partitions_2_splits_10_copies_2.txt

counter=0
while [ $counter -le 4 ] # Change back to 9 after
do
python3 create_data.py --seed $counter --sequence_length 100000000 --measure_performance False > /dev/null # Change to 10^8 after

# Base
python3 main.py --nodes $node_list --input sequence.npy --partitions 1 --splits 5 --copies 1 | grep -s "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/ddps_partitions_1_splits_5_copies_1.txt

# Different partitions & splits
#python3 main.py --nodes $node_list --input sequence.npy --partitions 1 --splits 10 --copies 1 | grep -s "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/ddps_partitions_1_splits_10_copies_1.txt
#python3 main.py --nodes $node_list --input sequence.npy --partitions 2 --splits 5 --copies 1 | grep -s "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/ddps_partitions_2_splits_5_copies_1.txt
#python3 main.py --nodes $node_list --input sequence.npy --partitions 2 --splits 10 --copies 1 | grep -s "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/ddps_partitions_2_splits_10_copies_1.txt

# Different copies
#python3 main.py --nodes $node_list --input sequence.npy --partitions 1 --splits 5 --copies 2 | grep -s "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/ddps_partitions_1_splits_5_copies_2.txt
#python3 main.py --nodes $node_list --input sequence.npy --partitions 1 --splits 10 --copies 2 | grep -s "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/ddps_partitions_1_splits_10_copies_2.txt
#python3 main.py --nodes $node_list --input sequence.npy --partitions 2 --splits 5 --copies 2 | grep -s "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/ddps_partitions_2_splits_5_copies_2.txt
#python3 main.py --nodes $node_list --input sequence.npy --partitions 2 --splits 10 --copies 2 | grep -s "time" | cut -d ":" -f 2 | cut -d " " -f 2 &>> Times/ddps_partitions_2_splits_10_copies_2.txt
((counter++))
done

# Different copies
python3 Times/experiments_ddps.py "Times/ddps_partitions_1_splits_5_copies_1.txt"

# Different partitions
#python3 Times/experiments_ddps.py "Times/ddps_partitions_1_splits_10_copies_1.txt"
#python3 Times/experiments_ddps.py "Times/ddps_partitions_2_splits_5_copies_1.txt"
#python3 Times/experiments_ddps.py "Times/ddps_partitions_2_splits_10_copies_1.txt"

# Different copies
#python3 Times/experiments_ddps.py "Times/ddps_partitions_1_splits_5_copies_2.txt"
#python3 Times/experiments_ddps.py "Times/ddps_partitions_1_splits_10_copies_2.txt"
#python3 Times/experiments_ddps.py "Times/ddps_partitions_2_splits_5_copies_2.txt"
#python3 Times/experiments_ddps.py "Times/ddps_partitions_2_splits_10_copies_2.txt"

