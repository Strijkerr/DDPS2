import sys
import pickle

filename = sys.argv[1]
infile = open(filename,'rb')
dictionary = pickle.load(infile)
infile.close()
print(dictionary)