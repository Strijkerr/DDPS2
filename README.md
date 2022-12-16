# DDPS2

This repository was made for assignment 2 of the course: Distributed Data Processing Systems at Leiden University, LIACS.
It is a toy project that simulates MapReduce for a simple map and reduce job.

Given a sequence of digits, it can count the occurence per digit in the sequence in a distributed processing fashion.

It is made to work with the DAS-V cluster (https://www.cs.vu.nl/das5/).

Before you can run the script. You can generate a sequence of digits by running create_data.py first.

```console
e.g.: python3 create_data.py
```
The sequence is stored as 'sequence.npy'

You can run the main script from the command line by e.g.,: python3 main.py --nodes node115,node116,node117 --partitions 2 --copies 3

You can give it the following arguments:

List of nodes,  e.g.: --nodes node102,node103,node104
Input file name, e.g.: --input sequence.npy
Number of partitions, e.g.: --partitions 1
Input file splits, e.g.: --splits 5
Copies, e.g.: --copies 1

Tip!:
If you activate the script too frequently, you can get a: "[Errno 98] Address already in use" error.
In that case, just wait a little bit (~10s) before running the script again.
