#!/C:\Users\Administrator\PycharmProjects\pythonProject\venv\Scripts python3.9
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: ppDowntime.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 8月 02, 2021
# ---
import configparser
import datetime
import time

import pandas
import requests
import sqlalchemy


def getRecord(url1, token1):  # 从URL获取Json文件
    headers = {"Authorization": token1,
               "Content-Type": "application/json; charset=utf-8",
               "pageindex": "0",
               "pageSize": "20",
               # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:44.0) Gecko/20100101 Firefox/44.0",
               # "Accept": "application/json, text/javascript, */*; q=0.01",
               # "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
               # "Accept-Encoding": "gzip, deflate, br",
               # "X-Requested-With": "XMLHttpRequest",
               # #"Cookie": "xxx.............",  # 此处cookie省略了
               # "Connection": "keep-alive"
               }
    payload = {"registryCenterId": "96423ab8-65e9-440c-869f-1f0a9be0efcb"}
    r = requests.post(url1, json=payload, headers=headers, verify=False)
    r = r.json()
    print('获取数据成功')
    print('----------------')
    return r


def convertToPandas(data):  # 字典转为DateFrame
    pandas.set_option('display.max_columns', None)  # 显示所有列
    try:
        data = data["result"]
    except Exception as e:
        print(e)
        print("未正确获取json数据~")
        exit(1)
    columns = ["tjzsc", "dh", "fj", "ycms", "last_modified_date", "bm", "is_del", "id", "qtwb", "zrzg", "ca", "wcm",
               "gh", "lxdh", "opd", "zaleader", "yylx1", "za", "zc", "last_modified_by", "yxl", "created_by", "cq",
               "zrdw", "buiding", "xm", "zgyx", "ygyxje", "cz", "jsrq", "zt", "formdataid", "fa", "created_date",
               "fsrq", "wcrq"]
    df1 = pandas.DataFrame(columns=columns)
    for i in range(len(data)):
        row = list(data[i].values())
        df1.loc[i] = row
    for i in range(len(df1)):  # id为空时自动填充
        if df1.loc[i, "id"] is None:
            df1.loc[i, "id"] = str(i)
    print('转换数据成功')
    return df1


def insertIntoSql(frame, tableName2):
    # DataFrame数据插入PG sql
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    # connection = engine.raw_connection()
    print('链接PG SQL成功')
    frame.to_sql(tableName2, engine, index=False, if_exists='append')
    engine.dispose()
    print('数据插入PG SQL成功')


def conPGSQL(tableName1):  # 链接PG数据库，查询已处理的时段
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute(f'select distinct id from {tableName1};')
    row = cursor.fetchall()
    row = [''.join(i) for i in row]  # 元组转化为列表
    connection.commit()
    cursor.close()
    engine.dispose()
    return row


def date_compare(date1, date2, fmt) -> bool:  # 日期大小比较
    zero = datetime.datetime.fromtimestamp(0)
    try:
        d1 = datetime.datetime.strptime(date1, fmt)
    except:
        d1 = zero
    try:
        d2 = datetime.datetime.strptime(date2, fmt)
    except:
        d2 = zero
    print('日期比较成功')
    return d1 > d2


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('C:/ppDowntime/ppDowntime.ini')              # 导出配置文件
    token = config.get("messages", "token")
    url = config.get("messages", "url")
    tableName = "pp_downtime"
    sqlData = conPGSQL(tableName)
    json = getRecord(url, token)
    print(sqlData)
    df = convertToPandas(json)
    for i in range(len(df)):
        if df.loc[i, "id"] in sqlData:
            df.drop(i, axis=0, inplace=True)             # 去掉行多余数据
    if len(df) == 0:
        print("当日数据已导入数据库~")
    else:
        insertIntoSql(df, tableName)
    time.sleep(5)           # 暂停5秒
