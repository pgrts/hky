



x = 0
y = 1

lis = []

for each in range(16):
    if ((y-x) == 1) or ((y-x) == -1) or ((x-y) == 1) or ((x-y) == -1):
        lis.append(str(x) + 'v'+ str(y))
        lis.append(str(y) + 'v' + str(x))
    lis.append(str(x) + 'v' + str(y-1))
    x+=1
    y+=1
print(lis)

