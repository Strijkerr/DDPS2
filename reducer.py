import pickle
import sys
import collections

pickle_files = [sys.argv[1],sys.argv[2]] # First argument
total_dict = collections.Counter()

for pickle_file in pickle_files :
    with open(pickle_file, 'rb') as inputfile:
        pickle_dict = pickle.load(inputfile)
        total_dict+=pickle_dict

print(total_dict)