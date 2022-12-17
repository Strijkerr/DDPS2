sequences = [1000000, 10000000, 100000000]

for s in sequences :
    f = open(f"single_system_{s}.txt", "r")
    total = 0
    count = 0
    for x in f:
        total+=float(x)
        count+=1
    print(f"Final average {s}:",total/count)