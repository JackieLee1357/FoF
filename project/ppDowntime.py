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
import sys
import time
import pandas
import requests
import sqlalchemy


def getRecord(url1, token1):  # 从URL获取Json文件
    headers = {"Authorization": token1,
               "Content-Type": "application/json; charset=utf-8",
               "pageindex": "0",
               # "pageSize": "1000",  # 获取数据的数量
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
        sys.exit(1)
    columns = list(data[0].keys())
    # print(columns)
    df1 = pandas.DataFrame(columns=columns)
    for i in range(len(data)):
        row = list(data[i].values())
        df1.loc[i] = row

    df1["ygyxje"] = pandas.to_numeric(df1["ygyxje"])  # 更改数据类型
    df1["yxl"] = pandas.to_numeric(df1["yxl"])  # 更改数据类型
    print(df1.columns)
    # df1 = df1.fillna(value="NA")  # 填充错误
    # print(len(data1))
    # df1['created_date'] = [i[:22] for i in df1['created_date']]   # pandas分割字符串
    # df1['last_modified_date'] = [i[:22] for i in df1['last_modified_date']]
    for i in range(len(df1)):
        # if df1.loc[i, 'dh'] == '' or df1.loc[i, 'id'] == '':
        if df1.loc[i, 'id'] == '':
            df1 = df1.drop(index=i)
            print(f"第{i}行单号或id为空，删除数据~")
            print('-----------')
    df1.reset_index(drop=True, inplace=True)
    df1 = df1[["id", "tjzsc1", "dh", "fj", "ycms", "last_modified_date",
     "bm", "is_del", "qtwb", "zrzg", "ca", "wcm",
     "gh", "lxdh", "oPD", "zaleader",
     "yylx1", "za", "zc", "last_modified_by", "yxl",
     "created_by", "cq", "zrdw", "buiding",
     "xm", "zgyx", "ygyxje", "cz",
     "jsrq1", "zt", "formDataid", "fa", "created_date", "fsrq1", "wcrq1"
     ]]
    # print(df1.columns)
    df1["wcrq1"] = df1["fsrq1"]
    # df1 = df1.replace(to_replace="", value="1900-00-00T00:00:00.000Z")           # 替换空值

    print('转换数据成功')
    print('-----------')
    return df1


def insertIntoSql(frame, tableName2):
    # DataFrame数据插入PG sql
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    # connection = engine.raw_connection()
    print('链接PG SQL成功')
    # print(frame)
    frame.to_sql(tableName2, engine, index=False, if_exists='append', method='multi', index_label=None,
                 chunksize=None,)
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
    config.read('C:/ppDowntime/ppDowntime.ini')  # 导出配置文件
    token = config.get("messages", "token")
    url = config.get("messages", "url")
    tableName = "pp_downtime"
    sqlData = conPGSQL(tableName)  # 从数据库获取已存在的单号信息
    json = getRecord(url, token)
    print(json)
    df = convertToPandas(json)
    print(df)
    for i in range(len(df)):
        if df.loc[i, "id"] in sqlData:
            df.drop(i, axis=0, inplace=True)  # 去掉数据库已存在数据
    if len(df) == 0:
        print("当日数据已导入数据库，请勿重复导入~")
    else:
        print(df)
        insertIntoSql(df, tableName)
    time.sleep(5)  # 暂停5秒
