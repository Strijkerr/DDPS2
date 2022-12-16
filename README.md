# DDPS2

This repository was made for assignment 2 of the course: Distributed Data Processing Systems at Leiden University, LIACS.
The project involves a toy distributed system that simulates MapReduce for a simple task: given a sequence of digits, count the occurence per digit in the sequence. It is made to work with the DAS-V cluster (https://www.cs.vu.nl/das5/).

1. Before you can run the script. You must generate a sequence of digits by running create_data.py first, e.g.:

```console
python3 create_data.py
```

The sequence is stored as 'sequence.npy'

2. You can then run the main script from the command line by e.g.,: 

```console
python3 main.py --nodes node115,node116,node117 -- input sequence.npy --partitions 2 --splits 5 --copies 3
```

3. The following arguments can be modified:

List of nodes. The first node in the sequence will become the master node, the rest will become workers.  e.g.: 
```console
--nodes node102,node103,node104
```
Input file name. Must exist in the working directory. Default is: 'sequence.npy'. 
```console
--input sequence.npy
```
Number of partitions. Specifies the number of reduce tasks, and reduce task results. Does not effect final output, as it will aggregate over all reduce task results in case of multiple, e.g.: 

```console
--partitions 1
```
Input file splits, e.g.: --splits 5

Copies, e.g.: --copies 1

4. This project makes use of the following ports: 22, 56609. These were chosen somewhat trivially.

Tip!:
If you activate the script too frequently, you can get a: "[Errno 98] Address already in use" error.
In that case, just wait a little bit (~10s) before running the script again.
