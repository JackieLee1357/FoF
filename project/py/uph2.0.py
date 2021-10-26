# -*- coding: utf-8 -*- 
# @Time : 2021/5/24 14:13 
# @Author : chang
# @File : uph2.0.py
import pyodbc
import pandas
import datetime
import configparser
import time
import sqlalchemy
import io
import sqlalchemy.sql.default_comparator


def link_select_sql(con, start_time, next_time):
    """
    查询sql，
    :param con: 链接，写在配置文件上
    :param sql: 查询语句
    :return: dataframe 格式的数据查询结果
    """
    sql = '''SELECT expr1 as number ,
    SUBSTRING(bg,12,6) as bgeeee,
    event,lastupdate,
    stationid,process FROM [Trace_D16].[dbo].[V_History_Temp] with (nolock)
    where  lastupdate >'{time_next_sql}' and lastupdate <='{time_start_sql}'
    and expr1 not in (SELECT expr1 from [Trace_D16].[dbo].[V_History_Temp] WITH(NOLOCK) where process = 'doe-in')
    order by lastupdate asc '''.format(time_start_sql=start_time, time_next_sql=next_time)

    conn = pyodbc.connect(r'{conn}'.format(conn=con))
    print(sql)
    cur = conn.cursor()
    cur.execute(sql)

    row = cur.fetchall()
    conn.close()
    return row


# def select_yesterday(con, time_1, time_2, time_3):
#     """
#
#     :param con: 连接
#     :param time_1: 离现在时间最近一天
#     :param time_2: 最近一天减一
#     :param time_3: 最近一天减二
#     :return: row查询结果
#     """
#     conn = pyodbc.connect(r'{conn}'.format(conn=con))
#     print('link success')
#     cur = conn.cursor()
#     sql = '''SELECT expr1 as number ,
#     SUBSTRING(bg,12,6) as bgeeee,
#     event,lastupdate,
#     stationid,process FROM [Trace_D16].[dbo].[V_History_Temp] with (nolock)
#     where lastupdate <'{time_next}' and lastupdate >='{time_yesterday}'
#     and process = 'oqc-out'
#     and  expr1 not in (SELECT expr1 from [V_History_Temp] WITH(NOLOCK) where lastupdate >='{time_next}' and lastupdate <'{time_start}')
#     '''.format(time_start=time_1, time_next=time_2, time_yesterday=time_3)
#     # time_start=time_1,
#     print(sql)
#     cur.execute(sql)
#     row = cur.fetchall()
#     conn.close()
#
#     return row


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
    cursor.copy_from(output, 'uph_phoenix_2', null='')
    connection.commit()
    cursor.close()
    engine.dispose()


def del_sql(time_1, time_2, time_3, time_4):
    """

    :param time_4: -1
    :param time_3: -1
    :param time_1:今天日期
    :param time_2:昨天日期
    :return:
    """
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    print('link success')
    connection = engine.raw_connection()
    cursor = connection.cursor()
    # 删除今天0-6 27
    sql = '''delete  from uph_phoenix_2 t where t.date='{a}' and
                  t.hour  in ('0','1','2','3','4','5')'''.format(a=time_1)
    print(sql)
    # 删除昨天一天 26
    sql2 = '''delete  from uph_phoenix_2 t where t.date='{b}' '''.format(b=time_2)
    print(sql2)
    # 删除前天 25
    sql3 = '''delete  from uph_phoenix_2 t where t.date='{c}' '''.format(c=time_3)
    print(sql3)
    # 删除大前天 6-24
    sql4 = '''delete  from uph_phoenix_2 t where t.date='{d}' and
                     t.hour not in ('0','1','2','3','4','5')'''.format(d=time_4)
    print(sql4)

    cursor.execute(sql)
    connection.commit()
    cursor.execute(sql2)
    connection.commit()
    cursor.execute(sql3)
    connection.commit()
    cursor.execute(sql4)
    connection.commit()
    cursor.close()
    engine.dispose()


def change_data(data):
    if len(data) == 19:
        return data[11:15]
    else:
        return data[len(data) - 1:len(data)]


def change_dataframe(dataframe):
    dataframe = pandas.DataFrame.from_records(dataframe,
                                              columns=['sn', 'bg', 'event', 'lastupdate', 'stationid', 'process'])

    dataframe.drop_duplicates(subset=['sn', 'process'], keep='last', inplace=True)

    dataframe.reset_index(drop=True, inplace=True)
    dataframe['sn'] = dataframe['sn'].apply(change_data)
    dataframe['date'] = dataframe['lastupdate'].astype('str').str[0:10]
    dataframe['hour'] = dataframe['lastupdate'].astype('str').str[11:13]
    dataframe.drop(columns=['lastupdate'], inplace=True)
    dataframe = dataframe.fillna('')
    dataframe['number'] = dataframe.groupby(
        by=['sn', 'stationid', 'bg', 'date', 'hour', 'event', 'process']).cumcount()+1

    dataframe = dataframe.sort_values(by=['number'])
    # print(dataframe)

    dataframe = dataframe.drop_duplicates(subset=['sn', 'stationid', 'bg', 'date', 'hour', 'event', 'process'],
                                          keep='last')

    dataframe.insert(0, 'field', dataframe['number'])
    dataframe.insert(4, 'hour1', dataframe['hour'])
    dataframe.insert(5, 'date1', dataframe['date'])
    dataframe.pop('number')
    dataframe.pop('hour')
    dataframe.pop('date')
    return dataframe


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('c:\\Phoenix\\uph_Phoenix.ini')
    time_start = config.get("messages", "time_start")
    time_test= config.get("messages", "time_test")
    con = config.get("messages", "link_sql")
    time_start = datetime.datetime.strptime(time_start, '%Y-%m-%d %H:%M:%S')
    time_test=datetime.datetime.strptime(time_test, '%Y-%m-%d %H:%M:%S')
    time_start_cut_hour = time_start + datetime.timedelta(hours=-1)
    time_start_add_hour=time_start + datetime.timedelta(hours=+1)
    time_start_cut_one_day = time_start + datetime.timedelta(days=-1)
    time_start_cut_two_day = time_start + datetime.timedelta(days=-2)
    time_start_cut_three_day = time_start + datetime.timedelta(days=-3)
    pandas.set_option('display.max_columns', None)
    pandas.set_option('display.max_rows', None)
    # 增加一个小时的数据
    a = link_select_sql(con=con, start_time=time_start, next_time=time_start_cut_hour)
    a = change_dataframe(dataframe=a)
    insert_sql(data=a)
    if str(time_start)[11:13] == '06':
        print('此次为清空次')
        del_sql(time_1=str(time_start)[0:10], time_2=str(time_start_cut_one_day)[0:10],
                time_3=str(time_start_cut_two_day)[0:10], time_4=str(time_start_cut_three_day)[0:10])

        three_day_data = link_select_sql(con=con, start_time=str(time_start),
                                         next_time=str(time_start_cut_three_day))

        three_day_date = change_dataframe(three_day_data)
        insert_sql(data=three_day_date)
    config.set("messages", "time_start", str(time_start_add_hour))

    config.write(open('c:\\Phoenix\\uph_Phoenix.ini', "r+", encoding="utf-8"))
    print('时间已更新,运行下次')
