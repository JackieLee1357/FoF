#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: traceMqConsumer.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 12月 21, 2021
# ---
import datetime
import re
from time import sleep
import pandas as pd
from clickhouse_driver import Client
from kafka import KafkaConsumer

host = 'CNWGPM0HOUSE81'  # 服务器地址
port = 9000  # 端口
user = 'oeuser'  # 用户名
password = 'oeuser123456'  # 密码
database = 'JGPWMD49'  # 数据库
send_receive_timeout = 60  # 超时时间
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


def insertToClk(database, tableName, a):
    if tableName == "TraceMQResult":
        a["createtime"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 数据插入数据库时间
    data = {}
    type_dict = get_type_dict(tableName)
    columns = list(type_dict.keys())
    # 类型处理
    for i in range(len(columns)):
        col_name = columns[i]
        col_type = type_dict[col_name]
        if 'Date' in col_type:
            try:
                data[col_name] = datetime.datetime.strptime(a[col_name], "%Y-%m-%d %H:%M:%S")
            except:
                try:
                    data[col_name] = datetime.datetime.strptime(a[col_name], "%m-%d-%Y %H:%M:%S")
                except:
                    data[col_name] = datetime.datetime.strptime(a[col_name], "%m/%d/%Y %H:%M:%S %p")
        elif 'Int' in col_type:
            data[col_name] = int(a[col_name])
        elif 'Float' in col_type:
            data[col_name] = float(a[col_name])
        elif col_type == 'String':
            data[col_name] = str(a[col_name])
    cols = list(data.keys())
    cols = ','.join(cols)
    client.execute(f"INSERT INTO {database}.{tableName} ({cols}) VALUES", [data], types_check=True)


# if __name__ == '__main__':
def traceMqConsumer(topic, tableName):
    # Kafka消费者
    # topic = 'traceMq'
    consumer = KafkaConsumer(bootstrap_servers=['10.127.3.133:9092'], group_id="test",
                             auto_offset_reset="earliest")
    consumer.subscribe(topic)
    # 参数bootstrap_servers：指定kafka连接地址
    # 参数group_id：如果2个程序的topic和group_id相同，那么他们读取的数据不会重复，2个程序的topic相同，group_id不同，那么他们各自消费相同的数据，互不影响
    # 参数auto_offset_reset：默认为latest表示offset设置为当前程序启动时的数据位置，earliest表示offset设置为0，在你的group_id第一次运行时，还没有offset的时候，给你设定初始offset。一旦group_id有了offset，那么此参数就不起作用了
    # path0 = "D:/test/faildata/"
    path0 = "/usr/local/airflow/dags/Kafka/faildata/"  # Linux path
    print('数据存入%s表:' % tableName)
    print("开始运行Kafka消费者：")
    for msg in consumer:
        i = msg.offset  # 消息序列号
        recvV = str(msg.value)
        recvV = recvV.replace(r"\r\n", "")
        recvV = recvV.replace(r"\\", "")
        recvV = recvV.replace('null', "''")
        recvV = recvV.replace(r'"{', "{")
        recvV = recvV.replace(r'}"', "}")
        a = recvV[2:-1]
        try:
            data = eval(a)
            project = data["project"]
            database = 'JGPWM' + project
            del data["project"]
            insertToClk(database, tableName, data)
            sleep(0.0001)
            if i % 10000 == 0:
                print("成功消费第" + str(i) + "条数据~")
        except Exception as e:
            timeNow = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            path = path0 + tableName + str(timeNow) + '.json'
            print("第" + str(i) + "条数据插入数据库失败，报错信息为：" + str(e))
            try:
                f = open(path, 'w+')
                f.write(str(a) + '\n')
                f.close()
                print('数据存入%s文件夹' % path)
            except Exception as e:
                print("数据插入本地失败，报错信息为：" + str(e))


def runConsumerResult(**kwargs):
    topic = "traceMqResult"
    dbName = "TraceMQResult"
    traceMqConsumer(topic, dbName)


def runConsumerLog(**kwargs):
    topic = "traceMqlogs"
    dbName = "TraceMQLog"
    traceMqConsumer(topic, dbName)


def runConsumerHistory(**kwargs):
    topic = "traceMqHistory"
    dbName = "TraceMQHistory"
    traceMqConsumer(topic, dbName)
