#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: dfToClk.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 12月 30, 2021
# ---

import re
import datetime
import pandas as pd
import sqlalchemy
from clickhouse_driver import Client

host = 'CNWGPM0HOUSE81'  # 服务器地址
port = 9000  # 端口
user = 'oeuser'  # 用户名
password = 'oeuser123456'  # 密码
database = 'JGPWMD49'  # 数据库
send_receive_timeout = 120  # 超时时间
client = Client(host=host, port=port, user=user, password=password, database=database,
                send_receive_timeout=send_receive_timeout)


def read_sql(sql):
    """

    :param sql:
    :return:
    """
    data, columns = client.execute(
        sql, columnar=True, with_column_types=True)
    df0 = pd.DataFrame({re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})
    return df0


def get_type_dict(tb_name):
    """

    :param tb_name:
    :return:
    """
    sql = f"select name, type from system.columns where database='{tb_name.split('.')[0]}' and table='{tb_name.split('.')[1]}';"
    df0 = read_sql(sql)
    df0 = df0.set_index('name')
    type_dict = df0.to_dict('dict')['type']
    return type_dict


def toClick(df, tb_name):
    df.fillna('', inplace=True)  # 替换Nan值
    type_dict = get_type_dict(tb_name)
    columns = list(type_dict.keys())
    dfColumns = set(df.columns)
    try:
        dfColumns.remove('')  # 避免因df列有''而报错
    except:
        pass
    newColumns = dfColumns.difference(set(columns))  # set求差集
    if len(newColumns) > 0:  # 获取新列并添加到数据库表中
        createColumns(tb_name, list(newColumns))
    # 类型处理
    for i in range(len(columns)):
        col_name = columns[i]
        col_type = type_dict[col_name]
        if col_name in dfColumns:
            if 'Date' in col_type:
                df[col_name] = pd.to_datetime(df[col_name])
            elif 'Int' in col_type:
                df[col_name] = df[col_name].astype('int')
            elif 'Float' in col_type:
                df[col_name] = df[col_name].astype('float')
            elif col_type == 'String':
                df[col_name] = df[col_name].astype('str').fillna('')
    # df数据存入clickhouse
    cols = ','.join(dfColumns)
    data = df.to_dict('records')
    # print(f'INSERT INTO {tb_name} ({cols}) VALUES')
    client.execute(f"INSERT INTO {tb_name} ({cols}) VALUES", data, types_check=True)
    # print("写入数据库成功~")


def fromClick(sql):
    res = client.execute(sql)
    return res


def createColumns(tb_name, cols):  # 添加新列到数据库中
    tb_name_month = tb_name[0:-4] + '1Month'
    tb_name_months = tb_name[0:-4] + '6Months'
    for i in cols:
        # 同步添加字段到月度表和年度表中
        client.execute(f"alter table {tb_name} add column if not exists {i} String;")
        client.execute(f"alter table {tb_name_month} add column if not exists {i} String;")
        client.execute(f"alter table {tb_name_months} add column if not exists {i} String;")


def toPG(df, table):
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    df.to_sql(table, engine, index=False, if_exists='append', method='multi', index_label=None,
              chunksize=None)
    engine.dispose()
    # print('数据写入SQL Server成功')


def fromPG(sql):  # 链接PG数据库，查询已处理的时段
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute(sql)  #
    row = cursor.fetchall()
    # row = [i[0].strftime("%Y-%m-%d %H") + ':00:00' for i in row]  # 时间转化为字符串
    row = [''.join(a) for a in row]  # 元组转化为列表
    connection.commit()
    cursor.close()
    engine.dispose()
    return row


def conHourFromPG(sql):  # 链接PG数据库，查询已处理的时段
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute(sql)  #
    row = cursor.fetchall()
    row = [i[0].strftime("%Y-%m-%d %H:%M:%S") for i in row]
    # row = [''.join(a) for a in row]  # 元组转化为列表
    connection.commit()
    cursor.close()
    engine.dispose()
    return row
