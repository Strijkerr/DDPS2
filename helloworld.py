import time
import sys
import json
import ast

json_dictionary = sys.argv[1:]
json_dictionary = ' '.join(json_dictionary)
json_dictionary = json.loads(json_dictionary)
print(json_dictionary)


# print("Type:", data)