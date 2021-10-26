# -*- coding: utf-8 -*- 
# @Time : 2021/4/7 16:27 
# @Author : chang
# @File : band.py
import configparser
import datetime
import io
import time
import numpy
import pandas
import pyodbc
import sqlalchemy

def linksql(con,sql):
    """
    查询sql，
    :param con: 链接，写在配置文件上
    :param sql: 查询语句
    :return: dataframe 格式的数据查询结果
    """

    conn=pyodbc.connect(r'{conn}'.format(conn=con))
    cur=conn.cursor()
    cur.execute(sql)
    row=cur.fetchall()
    conn.close()
    row=pandas.DataFrame.from_records(row)
    return row




def statas_change(i):
    '''
    :param i:status
    :return: change status 如果3后面的status不是2 则status改为3
    '''
    for x in range(len(i)-1):
        if i[x] == '3'and i[x+1]!='2':
            i[x+1] = '3'
    return i


def oee_ct(dsn):
    data_dsn=data_MSM[data_MSM['DSN']==str(dsn)]
    data_dsn=pandas.DataFrame.from_records(data_dsn)
    pandas.set_option('display.max_columns', None)
    pandas.set_option('display.max_rows', None)

    list_MSMStatus=data_dsn['Status'].to_list()
    for k in range(len(list_MSMStatus)-1):
        list_MSMStatus=statas_change(list_MSMStatus)
    data_dsn['Status']=list_MSMStatus
    data_dsn.drop(columns=['ID'],inplace=True)
    data_dsn.loc[(data_dsn['Status'] == '3')&(data_dsn['ErrorCode']==''),'ErrorCode']=numpy.nan
    data_dsn=data_dsn.fillna(method='ffill').\
        reset_index(drop=True)
    list_notin=data_dsn[data_dsn['Status'].isin(['3','4','5','6'])]
    data_dsn.drop(list_notin.index, inplace=True)
    data_dsn.drop_duplicates(subset=['TotalNum','Status'],keep='first',inplace=True)
    print(data_dsn)
    #方案：datadsn是 1 2 的条例，可以计算两者时间差，保存到sql中，然后找到下一段时间，上一个时间和下一个时间中间衔接,








if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('c:\\OEE\\OEE.ini')
    time_start = datetime.datetime.strptime(config.get("messages", "time_start"), '%Y-%m-%d %H:%M:%S')  # time_start: 开始时间
    time_next = time_start + datetime.timedelta(hours=+1)  # time_next:  开始时间的下一个小时
    time_end = datetime.datetime.strptime(config.get("messages", "time_end"), '%Y-%m-%d %H:%M:%S')  # time_end:结束时间
    time_end_next = time_end + datetime.timedelta(hours=+1)  # time_end_next:结束时间的下一个小时
    x = time.strftime('%Y-%m-%d %H:00:00')
    time_new = datetime.datetime.strptime(x, '%Y-%m-%d %H:00:00')  # time_new：现在整点时间
    SQL_MSM = f"""SELECT dSn,ID,Station,MachineName,Status,ErrorCode,TotalNum,EventTime FROM
     viewSMEAPI_MachineStatusMonth t WHERE EventTime>'{str(time_start)}' and  EventTime<'{str(time_end)}' 
     and Station not like '%EPZ%' order by EventTime asc"""
    # SQL_MSM: viewSMEAPI_MachineStatusMonth

    SQL_PIM = f'''select t.dSn,t.Station,t.Process,t.MachineName ,t.ProcessCycleTm,t.TotalNum,
    t.EventTime from viewSMEAPI_CNCProductInfoMonth t where t.EventTime>'{time_start}' and  t.EventTime<'{time_end}'  
    and t.Station not like '%EPZ%' and t.ProcessCycleTm<='500'  order by EventTime asc'''
    # SQL_PIM:  viewSMEAPI_CNCProductInfoMonth
    print(SQL_MSM)
    print(SQL_PIM)
    con = config.get("messages", "link_sql")
    data_MSM=linksql(con=con,sql=SQL_MSM)
    data_PIM=linksql(con=con,sql=SQL_PIM)
    data_MSM=pandas.DataFrame.from_records(data_MSM)
    data_MSM.columns=['DSN','ID','Station','MachineName','Status','ErrorCode','TotalNum','EventTime']
    data_PIM=pandas.DataFrame.from_records(data_PIM)
    data_PIM.columns=['DSN','Station','Process','MachineName','ProcessCycleTm','TotalNum','EventTime']
    # print(data_MSM)
    # print(data_PIM)
    oee_ct(dsn=str(60010812))