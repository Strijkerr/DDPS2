import time
import sys
import json
import ast

json_dictionary = sys.argv[1:]
print(json_dictionary)
#json_dictionary = ''.join(json_dictionary)
#json_dictionary = dict( pair.split('=') for pair in str(json_dictionary).split(',') )
#print(json_dictionary)

# data = json.loads(json_dictionary)
# print("Type:", data)