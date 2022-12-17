# DDPS2

This repository was made for Assignment 2 of the course: Distributed Data Processing Systems at Leiden University, LIACS.
The project involves a toy distributed system that simulates MapReduce for a simple task: given a sequence of digits, count the occurence per digit in the sequence. It is made to work with the DAS-V cluster (https://www.cs.vu.nl/das5/).

1. Before you can run the script. You must generate a sequence of digits by running create_data.py first. Arguments below are default values.

```console
python3 create_data.py --seed 0 --filename sequence --sequence_length 100000000 --measure_performance True
```

The sequence is stored as 'sequence.npy'

2. You can then run main.py script from the command line. E.g.:

```console
python3 main.py --nodes node115,node116,node117 --input sequence.npy --partitions 1 --splits 5 --copies 2
```

3. The following arguments can be modified:

List of nodes. The first node in the sequence will become the master node, the rest will become workers. Minimum is two nodes, maximum is 6/7 because of a bug. E.g.:
```console
--nodes node102,node103,node104
```
Input file name. Must exist in the working directory. Default is: 'sequence.npy'. E.g.:
```console
--input sequence.npy
```
Number of partitions. Specifies the number of reduce tasks, and reduce task results. Does not effect final output, as the script will aggregate over all reduce task results in case of multiple. Default is: 1. E.g.:

```console
--partitions 1
```
Input file splits. Specifies into how many shards the input ('sequence.npy') shall be split. Splits = number of map tasks.  Default is: 5. E.g.:

```console
--splits 5
```

Number of input split copies. Specifies how many duplicate shards shall be made. This was implemented to support fault-tolerance, however fault-tolerance ended up not being implemented. A value of 1 means, no copies will be made. Default is: 1. E.g.:

```console
--copies 1
```
4. This project makes use of the following ports: 22 and 56609. These ports were chosen somewhat trivially and be changed accordingly. The final result will be outputted to stdout. The (partial) results will be stored in the 'output' folder in the working directory as well.

Tip!: If you activate the script too frequently, you can get a: "[Errno 98] Address already in use" error. In that case, just wait a little bit (~10s) before running the script again. Should be solved now by allowing reuse of address by server socket (something with the server socket not closing but going into the TIME_WAIT state). 
