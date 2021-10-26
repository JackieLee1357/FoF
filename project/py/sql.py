# -*- coding: utf-8 -*- 
# @Time : 2021/4/29 15:14 
# @Author : chang
# @File : sql.py
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





if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('c:\\OEE\\OEE.ini')
    time_start = datetime.datetime.strptime(config.get("messages", "time_start"), '%Y-%m-%d %H:%M:%S')  # time_start: 开始时间
    time_next = time_start + datetime.timedelta(hours=+1)  # time_next:  开始时间的下一个小时
    time_end = datetime.datetime.strptime(config.get("messages", "time_end"), '%Y-%m-%d %H:%M:%S')  # time_end:结束时间
    time_end_next = time_end + datetime.timedelta(hours=+1)  # time_end_next:结束时间的下一个小时
    x = time.strftime('%Y-%m-%d %H:00:00')
    time_new = datetime.datetime.strptime(x, '%Y-%m-%d %H:00:00')  # time_new：现在整点时间
    SQL_MSM = '''SELECT dSn,ID,Station,MachineName,Status,ErrorCode,TotalNum,EventTime FROM
     viewSMEAPI_MachineStatusMonth t WHERE EventTime>'{start_time}' and  EventTime<'{endtime}' 
     and Station not like '%EPZ%' order by EventTime asc'''.format(start_time=str(time_start),endtime=str(time_end))
    # SQL_MSM: viewSMEAPI_MachineStatusMonth
    SQL_PIM = '''select t.dSn,t.Station,t.Process,t.MachineName ,t.ProcessCycleTm,t.TotalNum,
    t.EventTime from viewSMEAPI_CNCProductInfoMonth t where t.EventTime>'{start_time}' and  t.EventTime<'{endtime}'  
    and t.Station not like '%EPZ%' and t.ProcessCycleTm<='500'  order by EventTime asc'''.format(start_time=time_start,endtime=time_end)
    # SQL_PIM:  viewSMEAPI_CNCProductInfoMonth
    con = config.get("messages", "link_sql")
    data_MSM=linksql(con=con,sql=SQL_MSM)
    data_PIM=linksql(con=con,sql=SQL_PIM)
    print(data_MSM)
    print(data_PIM)
