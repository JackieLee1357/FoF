# -*- coding: utf-8 -*- 
# @Time : 2021/4/29 15:15 
# @Author : chang
# @File : uph_Phoenix.py
import pyodbc
import pandas
import datetime
import configparser
import time
import sqlalchemy
import io



def linksql(con,sql):
    """
    查询sql，
    :param con: 链接，写在配置文件上
    :param sql: 查询语句
    :return: dataframe 格式的数据查询结果
    """

    conn=pyodbc.connect(r'{conn}'.format(conn=con))
    print('link success')
    cur=conn.cursor()
    cur.execute(sql)
    row=cur.fetchall()
    conn.close()

    return row

def insert_sql(data):
    """

    :param data:
    :return: insert sql
    """
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    print('link success')
    connection = engine.raw_connection()
    cursor = connection.cursor()
    output = io.StringIO()
    data.to_csv(output, sep='\t', index=False, header=False)
    output.seek(0)
    cursor.copy_from(output, 'uph_phoenix', null='')
    connection.commit()
    cursor.close()
    engine.dispose()


def deletesql(del_sql_t,del_sql_y,insert_sql):
    print("删除昨天数据并重新导出一天数据")
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    cur = connection.cursor()
    cur.execute(del_sql_t)
    connection.commit()
    cur.execute(del_sql_y)
    connection.commit()
    a=linksql(con=con,sql=insert_sql)
    output = io.StringIO()
    a=pandas.DataFrame.from_records(a)
    a.to_csv(output, sep='\t', index=False, header=False)
    output.seek(0)
    cur.copy_from(output, 'uph_phoenix', null='')
    connection.commit()
    cur.close()
    engine.dispose()
# for i in range (0,10):
if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('c:\\Phoenix\\uph_Phoenix.ini')
    time_start = config.get("messages", "time_start")
    time_start = datetime.datetime.strptime(time_start, '%Y-%m-%d %H:%M:%S')
    time_next = time_start + datetime.timedelta(hours=+1)
    time_next_con = datetime.datetime.strptime(config.get("messages", "time_next_con"), '%Y-%m-%d %H:%M:%S')
    time_next_con_ne = time_next_con + datetime.timedelta(hours=+1)
    time_new = time.strftime('%Y-%m-%d %H:00:00')
    time_new = datetime.datetime.strptime(time_new, '%Y-%m-%d %H:00:00')

    uph_sql = '''SELECT count(1) as number ,
                SUBSTRING(bg,12,5) as bgeeee,SUBSTRING(Expr1,12,5)  as sneeee,event,
                DateName(hour,lastupdate) as 'hour',CONVERT(varchar(100), lastupdate, 111) as date,
                stationid,process FROM [Trace_D16].[dbo].[V_History_Temp] with (nolock)
                where lastupdate >='{time_start}' and lastupdate <'{time_next}'
                and expr1 not in (SELECT expr1 from [Trace_D16].[dbo].[V_History_Temp] WITH(NOLOCK) where process = 'doe-in')
                 group by SUBSTRING(bg,12,5) ,SUBSTRING(Expr1,12,5) ,DateName(hour,lastupdate) ,CONVERT(varchar(100),lastupdate, 111) ,
                 stationid,process'''.format(time_start=time_start, time_next=time_next_con)

    con = config.get("messages", "link_sql")
    uph_data = linksql(con=con, sql=uph_sql)

    uph_data = pandas.DataFrame.from_records(uph_data,
                                             columns=['field', 'bg', 'sn', 'event','hour', 'date', 'station', 'process'])

    uph_data['hour'] = uph_data['hour'].astype('int')

    insert_sql(data=uph_data)

    time_dd = time.strftime('%Y-%m-%d 06:00:00')
    time_dd = datetime.datetime.strptime(time_dd, '%Y-%m-%d %H:00:00')
    time_dd_ll = time_dd + datetime.timedelta(days=-1)
    x = str(time_dd_ll)[0:10]
    y = str(time_dd)[0:10]

    del_sql_y = '''delete  from uph_phoenix t where t.date='{a}'and
                 t.hour not in ('0','1','2','3','4','5') '''.format(a=x)
    del_sql_t = '''delete  from uph_phoenix t where t.date='{b}' and
                  t.hour  in ('0','1','2','3','4','5')'''.format(b=y)
    select_sql_oneday = '''SELECT count(1) as number ,
                SUBSTRING(bg,12,5) as bgeeee,SUBSTRING(Expr1,12,5)  as sneeee,
                DateName(hour,lastupdate) as 'hour',CONVERT(varchar(100), lastupdate, 111) as date,
                stationid,process FROM [Trace_D16].[dbo].[V_History_Temp] with (nolock)
                where lastupdate >='{time_start_day}' and lastupdate <'{time_next_day}'
                and expr1 not in (SELECT expr1 from [Trace_D16].[dbo].[V_History_Temp] WITH(NOLOCK) where process = 'doe-in')
                 group by SUBSTRING(bg,12,5) ,SUBSTRING(Expr1,12,5) ,DateName(hour,lastupdate) ,CONVERT(varchar(100),lastupdate, 111) ,
                 stationid,process '''.format(time_start_day=time_dd_ll, time_next_day=time_dd)

    if time_new == time_dd:
        print('删除一天数据并重新插入一条数据')
        deletesql(del_sql_t=del_sql_t, del_sql_y=del_sql_y, insert_sql=select_sql_oneday)

    if time_new > time_next:
        config.set("messages", "time_start", str(time_next))
        config.set("messages", "time_next_con", str(time_next_con_ne))
        config.write(open('c:\\Phoenix\\uph_Phoenix.ini', "r+", encoding="utf-8"))

    print('此次已完成，请开始右键运行下一次')
