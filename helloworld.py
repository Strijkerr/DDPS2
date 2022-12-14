import sys
import pickle
import collections

filename = sys.argv[1]
partitions = int(sys.argv[2])
workers = sys.argv[3:]
infile = open(filename,'rb')
shard_locations = pickle.load(infile)
infile.close()

# Dictionary with tasks
tasks = dict.fromkeys(shard_locations.keys(),None)
for i in tasks.keys() :
    tasks[i] = {'status': None, 'worker': None}

# for p in range(partitions) :

print(workers)
print(partitions)