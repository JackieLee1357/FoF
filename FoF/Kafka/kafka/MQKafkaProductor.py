#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: MQKafkaProductor.py
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
from kafka import KafkaProducer
import json

# topic = 'mykafka'
topic = 'traceMq'
producer = KafkaProducer(bootstrap_servers='10.127.3.133:9092')
# value_serializer=lambda m: json.loads(m))  # 连接kafka


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
    i = 0
    with open(filepath, encoding='utf-8') as myFile:
        datas = []
        for chunk in each_chunk(myFile, separator=',{"id":'):
            # print('chunk:')
            data = '{"id":' + chunk
            if len(chunk) < 5: continue
            # print(data)  # not holding in memory, but printing chunk by chunk
            try:
                data1 = json.dumps(data).encode("utf-8")
                i += 1
                print(data1)
                producer.send(topic, data1)
            except Exception as e:
                print(e)
                unInsertData.append(data)
            # datas.append(dataDic)
            # if len(datas) % 50 == 0:
            #     # print(datas)
            #     pd1 = pd.DataFrame(datas)
            #     print(pd1)
            #     producer.send(topic, pd1)
            #     # to_sql(pd1, 'TraceMQResult')
            #     datas = []
        # print(datas)
    # producer.send(topic, unInsertData)
    print(i)
    return unInsertData


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)  # 显示所有列
    pd.set_option('display.max_rows', None)  # 显示所有行
    path = "//CNWGPM0PG81/Test/Result/"
    files = os.listdir(path)
    df = pd.DataFrame()
    for file in files:
        filePath = path + file
        print(filePath)
        data2 = read_json(filePath)
        print(len(data2))
        f = open("//CNWGPM0PG81/Test/unInsertData", 'w')
        for i in range(len(data2)):
            f.write(str(i) + '\n')
            f.write(data2[i])
        f.close()
