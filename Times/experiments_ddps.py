import sys

filename = sys.argv[1]
file = open(filename, "r")
total = 0
count = 0
for x in file:
    total+=float(x)
    count+=1
print(f"{filename}:",total/count)