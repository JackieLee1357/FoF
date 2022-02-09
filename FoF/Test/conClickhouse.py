#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: conClickhouse.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 12月 09, 2021
# ---


from clickhouse_driver import Client


def getKeys(data, arg):
    # 递归展开json，返回所有key
    for key, value in data.items():
        if isinstance(value, list):
            arg.append(key)
        elif isinstance(value, dict):
            getKeys(value, arg)
        else:
            arg.append(key)
    return arg


host = 'CNWGPM0HOUSE81'  # 服务器地址
port = 9000  # 端口
user = 'default'  # 用户名
password = 'oeuser123456'  # 密码
database = 'default'  # 数据库
send_receive_timeout = 30  # 超时时间
client = Client(host=host, port=port, user=user, password=password, database=database,
                send_receive_timeout=send_receive_timeout)

sql = 'select top (10) * from default.TraceMQResult'
sql1 = """select name, type from system.columns where table=TraceMQLog;"""
sql2 = """
drop table OEDB 
"""
sql3 = """
create table TraceMQLog(id String, data String, agent String, event String, created Date, serials String, defects String,
       project String, process String) ENGINE=MergeTree() ORDER BY serials;
"""  # 建log表
sql4 = """
create table TraceMQResult(id String, results String, project String, process String) ENGINE=MergeTree() ORDER BY id;
"""  # 建result表
sql5 = """
create table TraceMQHistory(id String, sn String, sntype String, agent String, process String, testresult String,
 event String, lineid String, starttime Date, endtime Date, fixtureid String, created Date, stationid String,
  project String) ENGINE=MergeTree() ORDER BY id;
"""  # 建result表

sql6 = """
show tables;
"""

# sql = """CREATE TABLE OEDB
# (
#     name1 integer,
#     name2 String
# ) ENGINE = Memory
# """
ans = client.execute(sql)
print(ans)


