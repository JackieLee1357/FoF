#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: MQTEST.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 12月 09, 2021
# ---
import datetime
import os
import re
import pandas as pd
from clickhouse_driver import Client
import json

host = 'CNWGPM0HOUSE81'  # 服务器地址
port = 9021  # 端口
user = 'oeuser'  # 用户名
password = 'oeuser123456'  # 密码
database = 'default'  # 数据库
send_receive_timeout = 30  # 超时时间
client = Client(host=host, port=port, user=user, password=password, database=database,
                send_receive_timeout=send_receive_timeout)


def read_sql(sql):
    data, columns = client.execute(
        sql, columnar=True, with_column_types=True)
    df0 = pd.DataFrame({re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})
    return df0


def get_type_dict(tb_name):
    sql = f"select name, type from system.columns where table='{tb_name}';"
    df0 = read_sql(sql)
    df0 = df0.set_index('name')
    type_dict = df0.to_dict('dict')['type']
    return type_dict


def to_sql(df, tb_name):
    type_dict = get_type_dict(tb_name)
    columns = list(type_dict.keys())
    # 类型处理
    for i in range(len(columns)):
        col_name = columns[i]
        col_type = type_dict[col_name]
        if 'Date' in col_type:
            df[col_name] = pd.to_datetime(df[col_name])
        elif 'Int' in col_type:
            df[col_name] = df[col_name].astype('int')
        elif 'Float' in col_type:
            df[col_name] = df[col_name].astype('float')
        elif col_type == 'String':
            df[col_name] = df[col_name].astype('str').fillna('')
    # df数据存入clickhouse
    cols = ','.join(columns)
    data = df.to_dict('records')
    client.execute(f"INSERT INTO {tb_name} ({cols}) VALUES", data, types_check=True)
    # print("写入数据库成功~")


def each_chunk(stream, separator):
    buffer = ''
    isFir = True
    while True:  # until EOF
        chunk = stream.read(1024)  # I propose 4096 or so
        if not chunk:  # EOF?
            yield buffer[:-1]
            break
        buffer += chunk
        while True:  # until no separator is found
            try:
                part, buffer = buffer.split(separator, 1)
            except ValueError:
                break
            else:
                if isFir:
                    isFir = False
                    yield part[7:]
                else:
                    yield part


def read_json(filepath, db):
    unInsertData = []
    with open(filepath, encoding='utf-8') as myFile:
        try:
            pd1 = pd.read_json(myFile)
            to_sql(pd1, db)
            print("Json文件整体插入数据库成功~")
        except Exception as e:
            print("读取Json文件报错，报错信息为：" + str(e))
            datas = []
            for chunk in each_chunk(myFile, separator='{"id":'):
                data = '{"id":' + chunk
                if data[-1] == ',':
                    data = data[:-1]
                if data[-1] == '[':
                    data = data[:-1]
                if data[-1] == ']':
                    data = data[:-1]
                if len(chunk) < 5: continue
                try:
                    dataDic = json.loads(data)
                except Exception as e:
                    dataDic = []
                    print(e)
                    unInsertData.append(data)
                datas.append(dataDic)
                if len(datas) > 200:
                    pd1 = pd.DataFrame(datas)
                    # print(pd1)
                    to_sql(pd1, db)
                    datas = []
            pd1 = pd.DataFrame(datas)
            if len(datas) > 0:
                to_sql(pd1, db)
            print("拆分数据写入数据库成功，失败" + str(len(unInsertData)) + "条~")
    return unInsertData


def main(path, db):
    pd.set_option('display.max_columns', None)  # 显示所有列
    pd.set_option('display.max_rows', None)  # 显示所有行
    path1 = "/home/1485928/tracemq2/unInsertData" + str(datetime.datetime.now()) + ".json"
    files = os.listdir(path)
    if files is None or (len(files) == 0):
        print("文件夹无Json文件~")
        return
    for file in files:  # 遍历文件夹
        filePath = path + file
        print("读取数据文件名" + filePath)
        data2 = read_json(filePath, db)
        f = open(path1, 'w')
        for i in range(len(data2)):
            f.write(str(i) + '\n')
            f.write(data2[i])
        f.close()
    return


# if __name__ == '__main__':
def runIstJsonToClk():
    timer = datetime.datetime.now()
    path0 = "/home/1485928/tracemq"
    paths = os.listdir(path0)
    for p in paths:  # 遍历Test文件夹
        path = path0 + "/" + p + '/'
        dbname = 'TraceMQ' + p
        print("读取数据文件夹：" + path)
        # print("插入数据数据库表名" + dbname)
        main(path, dbname)
    timer = datetime.datetime.now() - timer
    print('运行时间：' + '%.1f' % (timer.seconds / 60.0) + ' minutes')  # 保留小数点后一位
