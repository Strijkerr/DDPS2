import numpy as np
import collections
import sys
import pickle

shard = sys.argv[1]
test =  np.load(shard)
count = collections.Counter(test)
filename = shard.split('/')[-1].split('.')[0]
with open(f'temp/{filename}.pickle', 'wb') as outputfile:
    pickle.dump(count, outputfile)