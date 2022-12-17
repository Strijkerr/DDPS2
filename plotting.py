f = open("single_system.txt", "r")
total = 0
for index,x in enumerate(f):
    total+=float(x)
    print(index,x)