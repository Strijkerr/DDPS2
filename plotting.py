
sequences = [1000000, 10000000, 100000000, 1000000000]

for s in sequences :
    f = open(f"Times/single_system_{s}.txt", "r")
    total = 0
    for index,x in enumerate(f):
        total+=float(x)
        print(index)
    print(total)