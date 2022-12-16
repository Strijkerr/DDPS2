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
python3 main.py --nodes node115,node116,node117 --partitions 2 --copies 3
```
You can give it the following arguments:

List of nodes,  e.g.: --nodes node102,node103,node104
Input file name, e.g.: --input sequence.npy
Number of partitions, e.g.: --partitions 1
Input file splits, e.g.: --splits 5
Copies, e.g.: --copies 1

Tip!:
If you activate the script too frequently, you can get a: "[Errno 98] Address already in use" error.
In that case, just wait a little bit (~10s) before running the script again.
