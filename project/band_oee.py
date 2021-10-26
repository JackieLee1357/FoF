import configparser
import datetime
import io
import time

import numpy
import pandas
import pyodbc
import sqlalchemy

config = configparser.ConfigParser()
config.read('c:\\OEE1\\tsxt.ini')

time_start=config.get("messages", "greeting1")
ti = datetime.datetime.strptime(time_start,'%Y-%m-%d %H:%M:%S')
time_next=ti+datetime.timedelta(hours=+1)
time_next_two=config.get("messages", "greeting2")
ti_next_two=datetime.datetime.strptime(time_next_two,'%Y-%m-%d %H:%M:%S')
time_nextt_need=ti_next_two+datetime.timedelta(hours=+1)
file_name=datetime.datetime.now().strftime('%y%m%d%H%M') + '.csv'

time_new = time.strftime('%Y-%m-%d %H:00:00')
time_new=datetime.datetime.strptime(time_new,'%Y-%m-%d %H:00:00')
print(time_next)




sql1=config.get("messages", "greeting")
sql2=config.get("messages", "greeting3")
sql3="'  and t.Station not like '%EPZ%' order by EventTime asc"
sql4="'  and t.Station not like '%EPZ%' order by EventTime asc"
sql6="'  and t.Station not like '%EPZ%' and t.ProcessCycleTm<='500'  order by EventTime asc"
sql5=config.get("messages", "greeting4")
SQL=sql1+time_start+sql2+time_next_two+sql3
print(SQL)
SQL_CACHE='select * from oee_cache'
SQL_ct=sql1+time_start+sql2+time_next_two+ sql4
print(SQL_ct)
drt_ct_andnu=sql5+time_start+sql2+time_next_two+sql6
print(drt_ct_andnu)

time_new1=time_new
time_next1=time_next
time_nextt_need1=time_nextt_need
if time_new1>time_next1:
    config.set("messages", "greeting1", str(time_next1))
    config.set("messages", "greeting2", str(time_nextt_need1))
    config.write(open('c:\\OEE1\\tsxt.ini', "r+", encoding="utf-8"))


print("sql语句已生成，开始连接数据库")


conn = pyodbc.connect(r'DRIVER={SQL Server};SERVER=CNWXIM0TRSQLV4A;DATABASE=SMDP;UID=PBIuser;PWD=PBIUser123456;')
cur1=conn.cursor()
cur1.execute(SQL)
rows1=cur1.fetchall()

cur2=conn.cursor()
cur2.execute(SQL_ct)
rows3=cur2.fetchall()

cur3=conn.cursor()
cur3.execute(drt_ct_andnu)
row_ctandnu=cur3.fetchall()
print(row_ctandnu)
conn.close()

drt_NEWONE=pandas.DataFrame.from_records(rows1)
drt_ct=pandas.DataFrame.from_records(rows3)

# db_url='postgres+psycopg2://OE_User:Jabil123456@CNWXIM0WINSVC01:5438/Trace_T1'
# engine =sqlalchemy.create_engine(db_url)
# connection =engine.raw_connection()
# cur2=connection.cursor()
# cur2.execute(SQL_CACHE)
#
# rows_timeone=cur2.fetchall()
# cur2.close()

# cur3=connection.cursor()
#
# dre_cache=pandas.DataFrame.from_records(rows_timeone)#缓存表数据
drt_NEWONE=pandas.DataFrame.from_records((drt_NEWONE))#需要的一小时数据
#
drt_ct_andnu=pandas.DataFrame.from_records((row_ctandnu))
#
#
# dre_cache.columns=['DSN','id','process','station','machinename','status','errorcode','TotalNum','EventTime']
drt_NEWONE.columns=['DSN','id','process','station','machinename','status','errorcode','TotalNum','EventTime']
drt_ct.columns=['DSN','id','process','station','machinename','status','errorcode','TotalNum','EventTime']


drt_ct_andnu.columns=['DSN','station','process','machinename','ProcessCycleTm','TotalNum','EventTime']



drt_dsn=drt_NEWONE['DSN']
data31=numpy.unique(drt_dsn)#每一个dsn数据

det_ct_for=drt_ct_andnu['DSN']
det_ct_for_one=numpy.unique(det_ct_for)

def digui(i):
    for x in  range(len(i)-1):
        if i[x]=='3' and i[x+1]!='2':
            i[x + 1] = '3'
    return i
#
#
def oee_CT(dsn):
    try:
        data_dsn=drt_ct[drt_ct['DSN']==str(dsn)]
        data_dsn=pandas.DataFrame.from_records(data_dsn)
        lst_dsn = data_dsn['status']
        lst_dsn = list(lst_dsn)

        for k in range(len(lst_dsn) - 1):
            lst_dsn = digui(lst_dsn)
        data_dsn['status'] = lst_dsn
        data_dsn.loc[(data_dsn['status'] == '3') & (data_dsn['errorcode'] == ''), 'errorcode'] = numpy.nan


        data_dsn = data_dsn.fillna(method="ffill")
        data_dsn=data_dsn.reset_index(drop=True)
        df1=data_dsn[data_dsn['status'].isin(['3','4','5','6'])]

        data_dsn.drop(df1.index, inplace=True)
        data_dsn.drop(columns=['id'],inplace=True)
        data_dsn.drop_duplicates(subset=['TotalNum','status'],keep='first',inplace=True)
        lst_dsn = data_dsn['TotalNum']
        lst_dsn = list(lst_dsn)

        totalnum=data_dsn['TotalNum']#开始的total
        totalnum=list(totalnum)
        totalnumone=totalnum[0]
        data_dsn = data_dsn.reset_index(drop=True)
        onenum=data_dsn.loc[:0]   #第一行
        onenum=pandas.DataFrame.from_records(onenum)

        if onenum.loc[0,'status']=='2':
            onenum.loc[0,'TotalNum']=totalnumone
            onenum.loc[0,'EventTime']=ti
            onenum.loc[0,'status']='1'
            lend = pandas.concat([onenum, data_dsn], ignore_index=False, axis=0)
            lst_time = lend['EventTime']
            lst_time = list(lst_time)
            del lst_time[0]
            lst_time.insert(len(lst_time), time_next)
            lend['EventTime2'] = lst_time
            lend['EventTime'] = lend['EventTime'].astype('datetime64')
            lend['EventTime2'] = lend['EventTime2'].astype('datetime64')
            lend['time3'] = lend['EventTime2'] - lend['EventTime']
            lend['time1'] = lend['time3'].dt.total_seconds().astype(int)

            # if lend.loc[0,'status']=='1':
            lend['EventTime'] = lend['EventTime'].astype('str')
            lend['day_1'] = lend['EventTime'].str[0:10]
            lend['hour_1'] = lend['EventTime'].str[11:13]
            lend.drop(columns=['EventTime2', 'time3', 'EventTime', 'TotalNum'], inplace=True)
            df_group_mean = lend.groupby(['DSN', 'machinename', 'process', 'day_1', 'hour_1', 'status'],
                                         as_index=False).sum()
            lst_status = lend['status']
            lst_status = list(lst_status)
            x = lst_status.count('1')
            df_group_mean = pandas.DataFrame.from_records(df_group_mean)
            df_group_mean.loc[df_group_mean['status'] == '1', 'time1'] = df_group_mean.loc[df_group_mean['status'] == '1', 'time1']/(x-1)


        else:

            lend=data_dsn
            # lend.to_csv('c:\\OEE\\' + file_name, mode='a', index=0, header=0)
            lst_time=lend['EventTime']
            lst_time=list(lst_time)
            del lst_time[0]
            lst_time.insert(len(lst_time),time_next)
            lend['EventTime2']=lst_time
            lend['EventTime'] = lend['EventTime'].astype('datetime64')
            lend['EventTime2'] = lend['EventTime2'].astype('datetime64')
            lend['time3'] = lend['EventTime2'] - lend['EventTime']

            lend['time1'] = lend['time3'].dt.total_seconds().astype(int)

            # if lend.loc[0,'status']=='1':
            lend['EventTime'] = lend['EventTime'].astype('str')
            lend['day_1']=lend['EventTime'].str[0:10]
            lend['hour_1']=lend['EventTime'].str[11:13]
            lend.drop(columns=['EventTime2', 'time3','EventTime','TotalNum'], inplace=True)
            df_group_mean = lend.groupby(['DSN', 'machinename', 'process','day_1','hour_1','status'], as_index=False).sum()

            lst_status = lend['status']
            lst_status = list(lst_status)
            x = lst_status.count('1')

            df_group_mean=pandas.DataFrame.from_records(df_group_mean)
            df_group_mean.loc[df_group_mean['status'] == '1', 'time1'] = df_group_mean.loc[df_group_mean['status'] == '1', 'time1']/x


        df = df_group_mean[~df_group_mean['status'].str.contains('2')]
        df = df.reset_index(drop=True)

        x = df.loc[0, 'time1']
    except:
        x='0'
    return x
print(oee_CT(60064146))

# def oee_one(dsn):
#     data_dsn=drt_NEWONE[drt_NEWONE['DSN']==str(dsn)]
#     drt_dsn_cache=dre_cache[dre_cache['DSN']==str(dsn)]
#
#     lst_dsn=data_dsn['EventTime']
#     lst_dsn=list(lst_dsn)
#
#     lst_cache=drt_dsn_cache['EventTime'].values.tolist()
#     if len(lst_cache)==0:
#         print('发现新机台，开始计算新机台数据')
#
#         del lst_dsn[0]
#         lst_dsn.insert(len(lst_dsn),ti_next_two)
#
#         data_dsn['EventTime2']=lst_dsn
#         lst=data_dsn['status']
#         lst=list(lst)
#         for k in range(len(lst)-1):
#             lst=digui(lst)
#         data_dsn['status']=lst
#         data_dsn.loc[(data_dsn['status'] == '3') & (data_dsn['errorcode'] == ''), 'errorcode'] = numpy.nan
#         data_dsn=data_dsn.fillna(method="ffill")
#         data_pns_end = data_dsn.iloc[[len(lst) - 1], :]
#         data_pns_end = pandas.DataFrame.from_records(data_pns_end)
#         dsn = str(data_pns_end.loc[0, 'DSN'])
#         id = str(data_pns_end.loc[0, 'id'])
#         c = str(data_pns_end.loc[0, 'process'])
#         d = str(data_pns_end.loc[0, 'station'])
#         e = str(data_pns_end.loc[0, 'machinename'])
#         f = str(data_pns_end.loc[0, 'status'])
#         g = str(data_pns_end.loc[0, 'errorcode'])
#         h = str(data_pns_end.loc[0, 'TotalNum'])
#         i = str(data_pns_end.loc[0, 'EventTime'])
#         #
#
#
#         insertsql='insert into oee_cache(dsn, id, process, station, machinename, status, errorcode, "TotalNum", "EventTime")values(%s, %s, %s, %s, %s, %s, %s, %s, %s) '
#         val=(dsn,id,c,d,e,f,g,h,i)
#         cur3.execute(insertsql,val)
#         connection.commit()
#         print('数据插入成功')
#         data_dsn['EventTime'] = data_dsn['EventTime'].astype('datetime64')
#         data_dsn['EventTime2'] = data_dsn['EventTime2'].astype('datetime64')
#         data_dsn['time3'] = data_dsn['EventTime2'] - data_dsn['EventTime']
#         data_dsn['time1']=data_dsn['time3'].dt.total_seconds().astype(int)
#         data_dsn['EventTime'] = data_dsn['EventTime'].astype('str')
#         data_dsn['day_1']=data_dsn['EventTime'].str[0:10]
#         data_dsn['hour_1']=data_dsn['EventTime'].str[11:13]
#         data_dsn.drop(columns=['TotalNum','id', 'EventTime','EventTime2', 'time3'], inplace=True)
#
#
#         data_pnsy_sum = data_dsn.groupby(['process', 'machinename', 'DSN', 'status', 'errorcode', 'day_1', 'hour_1'],as_index=False).sum()
#
#         data_dsn.drop(columns=['time1'])
#         zero_col_count=dict(data_dsn[['DSN','process','station','machinename','day_1','hour_1','status','errorcode']].value_counts())
#
#         y=tuple(zero_col_count.values())
#         y=list(y)
#         data_pnsy_sum=pandas.DataFrame.from_records(data_pnsy_sum)
#         data_pnsy_sum['numberstatus']=y
#         data_pnsy_sum_matrix = numpy.mat(data_pnsy_sum)
#         data_pnsy_sum_matrix_array = numpy.array(data_pnsy_sum_matrix)
#     else:
#
#         time_cache = lst_cache[0]
#         time_one = lst_dsn[0]
#         time_cache = str(time_cache)
#         time_one = str(time_one)
#     # time_cache=drt_dsn_cache.iloc[0,'EventTime']
#     # time_one=data_dsn.iloc[0,'EventTime']
#
#         from_date_time = datetime.datetime.strptime(time_cache[0:19], '%Y-%m-%d %H:%M:%S')
#         to_date_time = datetime.datetime.strptime(time_one[0:19], '%Y-%m-%d %H:%M:%S')
#         date_times = []
#         from_date_time = from_date_time.strftime('%Y-%m-%d %H:00:00')
#         to_date_time = to_date_time.strftime('%Y-%m-%d %H:00:00')
#
#         from_date_time = datetime.datetime.strptime(from_date_time, '%Y-%m-%d %H:%M:%S')
#         to_date_time = datetime.datetime.strptime(to_date_time, '%Y-%m-%d %H:%M:%S')
#         while from_date_time < to_date_time:
#             from_date_time += datetime.timedelta(hours=1)
#             date_times.append(from_date_time.strftime('%Y-%m-%d %H:00:00'))
#         # # list_time = list(date_times)
#         # time_cache=list(time_cache)
#
#         lend = pandas.DataFrame(
#             columns=['DSN', 'id', 'process', 'station', 'machinename', 'status', 'errorcode', 'TotalNum', 'EventTime'])
#         lend['EventTime'] = date_times
#         print(dsn)
#         p=len(date_times)
#         lend['status']=list(drt_dsn_cache['status'])*p
#         lend['DSN']=list(drt_dsn_cache['DSN'])*p
#         lend['id']=list(drt_dsn_cache['id'])*p
#         lend['process']=list(drt_dsn_cache['process'])*p
#         lend['station']=list(drt_dsn_cache['station'])*p
#         lend['machinename']=list(drt_dsn_cache['machinename'])*p
#         lend['errorcode']=list(drt_dsn_cache['errorcode'])*p
#         lend['TotalNum']=list(drt_dsn_cache['TotalNum'])*p
#
#
#         lend = pandas.concat([lend, data_dsn], ignore_index=False, axis=0)
#         lst = lend['status']
#         lst = list(lst)
#
#         for k in range(len(lst) - 1):
#             lst = digui(lst)
#             lend['status'] = lst
#         lend.loc[(lend['status'] == '3') & (lend['errorcode'] == ''), 'errorcode'] = numpy.nan
#         lend = lend.fillna(method="ffill")
#
#         data_pns_end = lend.iloc[[len(lst) - 1], :]
#
#         data_pns_end = pandas.DataFrame.from_records(data_pns_end)
#
#
#
#         x = str(data_pns_end.loc[0, 'DSN'])
#         y = str(data_pns_end.loc[0, 'status'])
#         z = str(data_pns_end.loc[0, 'errorcode'])
#         k = str(data_pns_end.loc[0, 'TotalNum'])
#         l = str(data_pns_end.loc[0, 'EventTime'])
#
#         updateSQL = 'UPDATE oee_cache SET status =' + "'" + y + "'" + ',' + 'errorcode=' + "'" + z + "'" + ',' + '"TotalNum"=' + "'" + k + "'" + ',' + '"EventTime"=' + "'" + l + "'" + 'where dsn=' + "'" + x + "'"
#
#
#         cur3.execute(updateSQL)
#         print(dsn+'数据更新')
#
#         connection.commit()
#
#         lst_eventime = lend['EventTime']
#         lst_eventime = list(lst_eventime)
#
#         del lst_eventime[0]
#         lst_eventime.insert(len(lst_eventime), ti_next_two)
#         lend['EventTime2'] = lst_eventime
#
#
#         lend['EventTime'] = lend['EventTime'].astype('datetime64')
#         lend['EventTime2'] = lend['EventTime2'].astype('datetime64')
#         lend['time3'] = lend['EventTime2'] - lend['EventTime']
#         lend['time1'] = lend['time3'].dt.total_seconds().astype(int)
#
#         lend['EventTime'] = lend['EventTime'].astype('str')
#         lend['day_1'] = lend['EventTime'].str[0:10]
#         lend['hour_1'] = lend['EventTime'].str[11:13]
#
#         lend.drop(columns=['TotalNum', 'id', 'EventTime', 'EventTime2', 'time3'], inplace=True)
#
#         data_pnsy_sum = lend.groupby(['process', 'machinename', 'DSN', 'status', 'errorcode', 'day_1', 'hour_1'],as_index=False).sum()
#
#         lend.drop(columns=['time1'], inplace=True)
#         zero_col_count = dict(
#             lend[['DSN', 'process', 'station', 'machinename', 'day_1', 'hour_1', 'status', 'errorcode']].value_counts())
#
#         y = tuple(zero_col_count.values())
#         y = list(y)
#
#         data_pnsy_sum = pandas.DataFrame.from_records(data_pnsy_sum)
#         data_pnsy_sum['numberstatus'] = y
#         data_pnsy_sum_matrix = numpy.mat(data_pnsy_sum)
#         data_pnsy_sum_matrix_array = numpy.array(data_pnsy_sum_matrix)
#
#     return data_pnsy_sum_matrix_array
#
#
#
#
# # def sc_number(pns_code):
# #     data_number=drt_NEWONE[drt_NEWONE['DSN']==str(pns_code)]
# #     data_number_1=dre_cache[dre_cache['DSN']==str(pns_code)]
# #
# #     data_number['EventTime'] = data_number['EventTime'].astype('str')
# #     data_number['day_1']=data_number['EventTime'].str[0:10]
# #     data_number['hour_1'] = data_number['EventTime'].str[11:13]
# #     station_i = numpy.unique(data_number['station'])
# #
# #     process_i = numpy.unique(data_number['process'])
# #
# #     dsn_i = numpy.unique(data_number['DSN'])
# #
# #     machine_i = numpy.unique(data_number['machinename'])
# #
# #     day_i=numpy.unique(data_number['day_1'])
# #
# #     hour_i=numpy.unique(data_number['hour_1'])
# #
# #     TotalNum_i=len(numpy.unique(data_number['TotalNum']))-1
# #     total_k=list(data_number['TotalNum'])[0]
# #     total_q=list(data_number_1['TotalNum'])
# #     if len(total_q)==0 or total_k!=total_q[0]:
# #         TotalNum_i = TotalNum_i
# #     else:
# #         TotalNum_i=TotalNum_i-1
# #
# #
# #     DF=pandas.DataFrame({'DSN':dsn_i,'process':process_i,'station':station_i,'machinename':machine_i,'day_1':day_i,'hour1':hour_i,'TotalNum':TotalNum_i})
# #     data_pnsy_sum_matrix = numpy.mat(DF)
# #     data_pnsy_sum_matrix_array1 = numpy.array(data_pnsy_sum_matrix)
# #     return data_pnsy_sum_matrix_array1
#
# def ct_and_nu(pos):
#     print(pos)
#     data_pns = drt_ct_andnu[(drt_ct_andnu['DSN'] == str(pos))]
#     data_pns['EventTime'] = data_pns['EventTime'].astype('str')
#     data_pns['day_1']=data_pns['EventTime'].str[0:10]
#     data_pns['hour_1']=data_pns['EventTime'].str[11:13]
#     data_pns.drop(columns=['EventTime','TotalNum'],inplace=True)
#
#     df_mean=data_pns.groupby(['DSN','station','process','machinename','day_1','hour_1'],as_index=False).mean()
#     df_mean = pandas.DataFrame.from_records(df_mean)
#     len_n=data_pns['ProcessCycleTm']
#     len_n=list(len_n)
#     len_n=len(len_n)
#     df_mean['TotalNum']=len_n
#     df_mean['cycleruntm']=oee_CT(pos)
#     df_mean_matrix=numpy.mat(df_mean)
#     df_mean_matrix_array=numpy.array(df_mean_matrix)
#     return df_mean_matrix_array
#
#
#
# fort=[]
# #
#
# for o in data31:
#     fort.extend(oee_one(o))
#
#
# fort=pandas.DataFrame.from_records(fort,columns=['process', 'machinename', 'DSN', 'status', 'errorcode', 'day_1', 'hour_1','time1','numberstatus'])
#
#
# sc_number_Z=[]
# for o in  det_ct_for_one:
#     sc_number_Z.extend(ct_and_nu(o))
# sc_number_Z=pandas.DataFrame.from_records(sc_number_Z,columns=['DSN','station','process','machinename','day_1','hour1','ProcessCycleTm','TotalNum','cycleruntm'])
# sc_number_Z=sc_number_Z[['DSN','station','process','machinename','day_1','hour1','TotalNum','ProcessCycleTm','cycleruntm']]
#
#
#
#
#
#
#
#
#
#
#
# # fort=[]
# #
# # for o in data31:
# #     fort.extend(oee_one(o))
# # fort=pandas.DataFrame.from_records(fort,columns=['process', 'machinename', 'DSN', 'status', 'errorcode', 'day_1', 'hour_1','time1','numberstatus'])
# # sc_number_Z=[]
# # for o in  data31:
# #     sc_number_Z.extend(sc_number(o))
# # sc_number_Z=pandas.DataFrame.from_records(sc_number_Z,columns=['DSN','process','station','machinename','day_1','hour1','TotalNum'])
# #
#
# # ct_oee=[]
# # for o in det_ct_for_one:
# #     ct_oee.extend(oee_CT(o))
# # ct_oee=pandas.DataFrame.from_records(ct_oee)
# # ct_oee.to_csv('c:\\OEE\\' + file_name, mode='a', index=0, header=0)
#
#
#
#
# print('导出成功')
# print('开始插入数据库')
# # db_url='postgres+psycopg2://OE_User:Jabil123456@CNWXIM0WINSVC01:5438/Trace_T1'
# # engine =sqlalchemy.create_engine(db_url)
# # connection =engine.raw_connection()
# cursor = connection.cursor()
# # # # #
# output=io.StringIO()
# # # #
# fort.to_csv(output, sep='\t',index = False, header = False)
# # output.getvalue()
# # # # #
# output.seek(0)
# cursor.copy_from(output, 'band_oee_new',null='')
# connection.commit()
#
#
# output=io.StringIO()
#
# connection =engine.raw_connection()
# sc_number_Z.to_csv(output, sep='\t',index = False, header = False)
# output.getvalue()
#
# output.seek(0)
#
# cursor = connection.cursor()
#
# cursor.copy_from(output, 'band_oee_pcs_new',null='')
# connection.commit()
#
#
# cursor.close()
# engine.dispose()
#
# print('END')
#
#
#
# print('此次已完成，请开始右键运行下一次')
