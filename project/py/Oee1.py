import numpy
import xlwt
import pandas
import xlwt
import pyodbc
import pandas
import datetime
import configparser
import time

##读取sql
config = configparser.ConfigParser()
config.read('c:\\OEE\\tsxt.ini')
time1 =time.strftime('%Y-%m-%d %H:00:00')
ti3 = datetime.datetime.strptime(time1,'%Y-%m-%d %H:%M:%S')
time4=ti3+datetime.timedelta(hours=-2)
time5=ti3+datetime.timedelta(hours=-1)
a=config.get("messages", "greeting")
b=config.get("messages", "greeting1")
c=config.get("messages", "greeting2")
d=config.get("messages", "greeting3")
e=config.get("messages", "greeting4")
sql=d+str(time5)+e
sql1=a+str(time5)+b+str(ti3)+c
print(sql)
print(sql1)
conn = pyodbc.connect(r'DRIVER={SQL Server};SERVER=CNWXIM0TRSQLV2A;DATABASE=SMDP;UID=PBIuser;PWD=PBIuser123456;')
cur = conn.cursor()
cur.execute(sql)
rows = cur.fetchall()#前两个小时的
#
cur1=conn.cursor()
cur1.execute(sql1)
rows1=cur1.fetchall()#前一个小时的
conn.close
drt=pandas.DataFrame.from_records(list(rows))#前两个小时
drt1=pandas.DataFrame.from_records(list(rows1))#前一个小时
#EventTime
drt.columns=['EventTime','DSN']
#
drt1.columns=['DSN','id','process','station','machinename','status','errorcode','TotalNum','EventTime']
#
drt21=drt1['DSN']
data31=numpy.unique(drt21)
#
def renode(pns_code):

    data_pns = drt1[(drt1['DSN'] == str(pns_code))]

    lst = data_pns['status'].values.tolist()  # 取出node列
    LIST_LST=list(lst)
    print(LIST_LST)
    print(lst)
    thr = []  # 3对应的索引
    two = []  # 2对应的索引
    for i in range(len(lst)):  # 遍历node列
        l = lst[i]  # 取出第i个索引的值
        if l == '3':  # 如果等于3
            thr.append(i)  # 存入列表
        elif l == '2':  # 如果等于
            two.append(i)  # 存入列表

    data = {}  # 新建字典
    for i in range(len(thr)):  # 遍历3的索引列表
        for j in range(len(two)):  # 遍历2的索引列表
            if thr[i] < two[j]:  # 如果3的索引小于2
                data[thr[i]] = two[j]  # 保存索引
                break  # 跳出循环data

    ndata = dict(sorted(data.items(), key=lambda x: x[0], reverse=True))  # 按3的索引排序

    res = {}  # 新建字典
    for k, v in ndata.items():  # 遍历结果
        if v not in res.values():  # 如果v不在res里
            res[k] = v  # 存入字典中

    # res
    for k, v in res.items():  # 遍历res字典
        lst[k:v] = [3] * len(lst[k:v])  # 更新lst的值


    data_pns['status1'] = lst  # 更新node列
    # print(data_pns)  # 输出结果
    data1_pns=drt[(drt['DSN'] == str(pns_code))]
    print(data1_pns)
    data1_pns_array=numpy.array(data1_pns)
    lst_last = data_pns['EventTime']#.values #现在时间的列
    lst_first=data1_pns_array[0,0]#.values #前面时间列
    # print(lst_first)
    # lst_first_list=list(lst_first)#前面时间改成列表
    # print(lst_first_list)
    lst_last_list=list(lst_last)#现在时间改成列表
    #
    # last_number = lst_first_list#找到前一个小时的尾数

    # print(last_number)
    del lst_last_list[len(lst_last_list) - 1]#删除现在小时的尾数
    lst_last_list.insert(0, lst_first)#现在小时添加之前的尾数
    lst_last_list_arrary = numpy.array(lst_last_list)
    data_pns['time2'] = lst_last_list_arrary

    file_name = datetime.datetime.now().strftime('%y%m%d%H%M') + '.csv'
    data_pns.to_csv('Z:\\PIS Data\\Ricky\\' + file_name, mode='a', index=0, header=0)

# def last_code(pns_code):
#     data_pns = drt[(drt['DSN'] == str(pns_code))]  # 取他的尾数
#     print(data_pns)
#     data1_pns = drt1[(drt1['DSN'] == str(pns_code))]#删除他的尾数
#     print(data1_pns)
#     lst_last=data_pns['EventTime'].values
#     lst_first=data1_pns['EventTime'].values
#     lst_first_list=list(lst_first)
#     lst_last_list=list(lst_last)
#     print(lst_first_list)
#     last_number=lst_last_list[len(lst_first_list)-1]
#     print(last_number)
#     del lst_first_list[len(lst_first_list)-1]
#     lst_first_list.insert(0,last_number)
#     lst_first_list_arrary=numpy.array(lst_first_list)
#     print(lst_first_list_arrary)
#     data1_pns['time2']=lst_first_list_arrary
#     print(data1_pns)
#
#
for i in data31:
    renode(i)

# renode(30000947)















