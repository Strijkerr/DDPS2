import time
import sys
import json

json_dictionary = sys.argv[1:]
data = json.loads(str(json_dictionary))
#print(json_dictionary)
print("Type:", data)