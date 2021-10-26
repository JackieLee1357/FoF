import pandas

data=pandas.read_excel('c:\\pc\\data.xls')

data1 = pandas.DataFrame(data, columns=['pns', 'id', 'zhic', 'dong', 'jiqi', 'error', 'number', 'time', 'node'])
# print(data1)

# print(data1.loc[:,['number']])
# print(data1.columns)
# print(data1.index)
data2 = data1[(data1.pns == 60010865)]

lst = data2['node'].values.tolist()

thr = []
two = []
for i in range(len(lst)):
    l = lst[i]
    if l == 3:
        thr.append(i)
    elif l == 2:
        two.append(i)

data = {}
for i in range(len(thr)):
    for j in range(len(two)):
        if thr[i] < two[j]:
            data[thr[i]] = two[j]
            break

ndata = dict(sorted(data.items(), key=lambda x: x[0], reverse=True))
# ndata
res = {}
for k, v in ndata.items():
    if v not in res.values():
        res[k] = v

# res
for k, v in res.items():
    lst[k:v] = [3] * len(lst[k:v])

data2['node'] = lst
print(data2)