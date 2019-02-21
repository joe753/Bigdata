samples = [
    (2001, 23),
    (2002, 7),
    (2002, -12),
    (2001, 21),
    (2003, 20),
    (2005, 13),
    (2003, 3),
    (2005, -2),
    (2003, 22),
    (2001, -3),
]

year = []
temp = []
res = []
i = 0
data = {}
for sample in samples:
    if sample[0] not in year:
        year.append(sample[0])
        temp.append([sample[1]])
    else :
        b = year.index(sample[0])
        temp[b].append(sample[1])

for i in temp:
    res.append(sorted(i))

for i, j in enumerate(year):
    data[j]= res[i]

for x, y in data.items():
    print (x, max(y))






