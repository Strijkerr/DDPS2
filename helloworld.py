import sys
import pickle

def returnDict (filename) :
    infile = open(filename,'rb')
    dictionary = pickle.load(infile)
    infile.close()
    return dictionary

shard_dict = returnDict(sys.argv[1])
task_dict = returnDict(sys.argv[2])
worker_dict = returnDict(sys.argv[3])

print(shard_dict)
print(task_dict)
print(worker_dict)