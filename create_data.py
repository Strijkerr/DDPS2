import numpy as np
import collections
import os

path = os.getcwd()
sequence_length = 1000000
sequence = np.random.randint(0, 9, sequence_length)
count = collections.Counter(sequence)
np.save(os.path.join(path, 'sequence'), sequence)