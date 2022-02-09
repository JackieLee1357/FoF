#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: consumer.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 12月 22, 2021
# ---


import threading
from Kafka.traceMqConsumer import traceMqConsumer

exitFlag = 0


class MyThread(threading.Thread):
    def __init__(self, topic, dbName):
        threading.Thread.__init__(self)
        self.topic = topic
        self.dbName = dbName

    def run(self):
        print("开始线程：" + self.name)
        traceMqConsumer(self.topic, self.dbName)
        print("退出线程：" + self.name)


if __name__ == '__main__':
    # 创建新线程
    topic1 = "traceMqResult"
    dbName1 = "TraceMQResult"
    Result = MyThread(topic1, dbName1)
    topic2 = "traceMqlogs"
    dbName2 = "TraceMQLog"
    Log = MyThread(topic2, dbName2)
    # topic3 = "traceMqHistory"
    # dbName3 = "TraceMQHistory"
    # History = MyThread(topic3, dbName3)

    # 开启新线程
    Result.start()
    Log.start()
    # History.start()
    Result.join()
    Log.join()
    # History.join()
    print("退出主线程")
