import sys
import pickle

filename = sys.argv[1]
infile = open(filename,'rb')
shard_locations = pickle.load(infile)
shards = shard_locations.keys()
infile.close()
print(shards)