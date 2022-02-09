#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: traceQueryDuplicates.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 1月 19, 2022
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


def fromClick(sql):
    res = client.execute(sql)
    return res


querySql = f"""SELECT id FROM JGPWMD49.TraceMQLog;"""
# print(querySql)
res = fromClick(querySql)
df = pd.DataFrame(res)  # 转为df
print(len(df))
df1 = df.drop_duplicates(subset=[0])
print(len(df1))
