import numpy as np
import collections
import os

seed = 0 
np.random.seed(seed)
path = os.getcwd()
sequence_length = 100000000
sequence = np.random.randint(0, 10, sequence_length)
count = collections.Counter(sequence)
print(count)
np.save(os.path.join(path, 'sequence'), sequence)