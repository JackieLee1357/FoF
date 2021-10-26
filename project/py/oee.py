
import pyodbc
import pandas
import datetime
import configparser
import time
import numpy


import pandas

file_name = datetime.datetime.now().strftime('%y%m%d%H%M') + '.csv'

data=pandas.read_csv('c:\\pk\\data.csv',header=None)

data=numpy.array(data)
data=pandas.DataFrame(data,columns=['DSN','id','process','station','machinename','status','errorcode','TotalNum','EventTime','NEWstatus','NEWeventime'])

# print(data)

# print(last_time)
new_time_list=data['EventTime'].values.tolist()
last_time_list=data['NEWeventime'].values.tolist()
print(last_time_list)
print(new_time_list)
# data['time3']=pandas.to_datetime(data['EventTime'],format='%Y-%m-%d %H:%M:%S',errors='coerce')-pandas.to_datetime(data['NEWeventime'],format='%Y-%m-%d %H:%M:%S',errors='coerce')
# print(data['time3'])
# data_time=datetime.datetime.strptime(data['time3'],'%Y-%m-%d')
# print(data_time)
# # print(last_time_list)
y=last_time_list[1]
print(y)
x=datetime.datetime.strptime(y)
print(x)
y=datetime.datetime.strptime(x).hour
print(y)
# for i in new_time_list:
#     time_need = i
#     x = datetime.datetime(str(i), "%Y-%m-%d %H:%M:%S")
#     print(i)
# for o in last_time_list:
#     time_need2 = o
#     y = datetime.datetime(o, "%Y-%m-%d %H:%M:%S")
#     print(o)


# z=(x-y).seconds
# print(z)
# startTime2 = datetime.datetime.strptime(time_need, '%Y-%m-%d %H:%M')

# endTime2 = datetime.datetime.strptime(time_need2, '%Y-%m-%d %H:%M')
# seconds = (endTime2 - startTime2).seconds
# total_seconds = (endTime2 - startTime2).total_seconds()
# print (total_seconds)
# data31=numpy.unique(data21)
# def xiu(pns_code):
#     data2 = data1[(data1.pns == pns_code)]  # 取出pns值
#     print(data2)
#     lst = data2['time'].values
#     print(lst)
#     lst_list=list(lst)
#     print(lst_list)
#     wei=lst_list[len(lst_list)-1]
#     print(wei)
#
#     del lst_list[len(lst_list)-1]
#     print(lst_list)
#     lst_list.insert(0,wei)
#     print(lst_list)
#     lst_list_array=numpy.array(lst_list)
#     data2['time2']=lst_list_array
#
#     file_name = datetime.datetime.now().strftime('%y%m%d%H%M') + '.csv'
#     data2.to_csv('c:\\pc\\'+file_name,mode='a',index=0,header=0)
#
#
# for i in data31:
#     xiu(i)
#     print(i)

def time_sum (end_time,):
    end_time = datetime.strptime(end_time, r"%Y/%m/%d %H:%M:%S")