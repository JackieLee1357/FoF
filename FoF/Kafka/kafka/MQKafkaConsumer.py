#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: MQKafkaConsumer.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 12月 17, 2021
# ---
import json
import os
import re
import pandas as pd
from clickhouse_driver import Client
from kafka import KafkaConsumer

host = 'CNWGPM0HOUSE81'  # 服务器地址
port = 9021  # 端口
user = 'oeuser'  # 用户名
password = 'oeuser123456'  # 密码
database = 'default'  # 数据库
send_receive_timeout = 5  # 超时时间
client = Client(host=host, port=port, user=user, password=password, database=database,
                send_receive_timeout=send_receive_timeout)
# 消费者
topic = 'traceMq'
consumer = KafkaConsumer(topic, bootstrap_servers=['10.127.3.133:9092'], group_id="test", auto_offset_reset="earliest")
# 参数bootstrap_servers：指定kafka连接地址
# 参数group_id：如果2个程序的topic和group_id相同，那么他们读取的数据不会重复，2个程序的topic相同，group_id不同，那么他们各自消费相同的数据，互不影响
# 参数auto_offset_reset：默认为latest表示offset设置为当前程序启动时的数据位置，earliest表示offset设置为0，在你的group_id第一次运行时，还没有offset的时候，给你设定初始offset。一旦group_id有了offset，那么此参数就不起作用了


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
    print("导入数据库成功~")


#
# def read_json(filepath):
#     with open(filepath, encoding='utf-8') as fp:
#         content = json.loads(fp.read())
#     return content


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


def read_json(filepath):
    unInsertData = []
    with open(filepath, encoding='utf-8') as myFile:
        datas = []
        for chunk in each_chunk(myFile, separator=',{"id":'):
            # print('chunk:')
            data = '{"id":' + chunk
            if len(chunk) < 5: continue
            # print(data)  # not holding in memory, but printing chunk by chunk
            try:
                dataDic = eval(data)
            except:
                unInsertData.append(data)
            datas.append(dataDic)
            if len(datas) % 50 == 0:
                # print(datas)
                pd1 = pd.DataFrame(datas)
                print(pd1)
                # producer.send(topic, pd1)
                to_sql(pd1, 'TraceMQResult')
                datas = []
        # print(datas)
    return unInsertData


if __name__ == '__main__':
    i = 0
    for msg in consumer:
        jsons = []
        i += 1
        # recv = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
        # recv = msg.value
        # print(recv)
        # print(type(recv))
        # js = json.loads(recv)
        print("json-----")
        print(msg)
        # print(js)
        # jsons.append(js)
        print("导入数据库中：")
        # pd1 = pd.DataFrame(js, )
        # print(pd1)
        # to_sql(pd1, 'TraceMQResult')
        # time.sleep(1)
        print("======"+str(i))



