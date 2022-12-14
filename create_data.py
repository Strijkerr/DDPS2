import numpy as np
import collections
import os
import argparse
import time

# Parsing command line arguments
def command_line_arguments () :
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--seed", default=0, type=int, help="E.g., 0, 1, 2")
    argparser.add_argument("--filename", default='sequence', type=str, help="E.g., sequence")
    argparser.add_argument("--sequence_length", default=1000000, type=int, help="E.g., 100000000") # Change default to >100000000 after testing
    return argparser.parse_args()

# Get command line arguments
args = command_line_arguments()
np.random.seed(args.seed)
path = os.getcwd()

# Generate random integers from [0-9] and save file.
sequence = np.random.randint(0, 10, args.sequence_length)
savepath = os.path.join(path, args.filename)
np.save(savepath, sequence)

# Measure performance from loading -> result
start = time.time()
sequence = np.load(savepath + '.npy')
count = collections.Counter(sequence)
end = time.time()
print(f"Elapsed time (s): {end-start}")
print(f"Results: {count}")