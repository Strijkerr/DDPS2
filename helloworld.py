import time
import sys
import json

json_dictionary = sys.argv[1]
with open(json_dictionary) as json_file:
    data = json.load(json_file)
 
    # Print the type of data variable
    print("Type:", data)