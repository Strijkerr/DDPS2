f = open("Times/ddps_partitions_1_splits_5_copies_1.txt", "r")
total = 0
count = 0
for x in f:
    total+=float(x)
    count+=1
print(f"Final average ddps_partitions_1_splits_5_copies_1:",total/count)