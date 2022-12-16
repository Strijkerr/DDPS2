import collections
import os
import pickle

total_dict = collections.Counter()
folderName = '/home/ddps2202/DDPS2/output/'
for file in os.listdir(folderName) :
    with open(folderName + file, "rb") as input_file:
        count = pickle.load(input_file)
        total_dict+=count
print(total_dict)