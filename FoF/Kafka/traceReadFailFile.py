#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: traceReadFailFile.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 1月 18, 2022
# ---
import os
from Trace.Kafka.traceMqConsumer import insertToClk


def fileToClk(path0, path1):
    raiseExpt = False
    expt = ''
    for root, dirs, files in os.walk(path0):
        for file in files:
            table = file[0:-19]
            path = root + file
            try:
                f = open(path, 'r+')
                data = f.read()
                f.close()
                data = eval(data)
                project = data["project"]
                database = 'JGPWM' + project
                insertToClk(database, table, data)
                os.remove(path)
                print(f"{file}数据插入{database}.{table}成功~")
            except Exception as e:
                expt = str(e)
                raiseExpt = True
                try:
                    f = open(path, 'r+')
                    data = f.read()
                    f.close()
                    path = path1 + file
                    f = open(path, 'w+')
                    f.write(str(data) + '\n')    # 如果失败，存入另一个文件夹~
                    f.close()
                    os.remove(root + file)
                    print('数据存入%s文件夹' % path)
                except Exception as e:
                    print("数据插入本地失败，报错信息为：" + str(e))
                continue
    if raiseExpt:
        print("读取本地文件插入数据库失败：" + expt)
        raise EOFError


# if __name__ == '__main__':
def RunReadFailFile(**kwargs):
    path0 = "/usr/local/airflow/dags/Kafka/faildata/"  # Linux path
    path1 = "/usr/local/airflow/dags/Kafka/faildata2/"  # Linux path
    # path0 = "D:/test/faildata/"
    # path1 = "D:/test/faildata2/"
    fileToClk(path0, path1)
