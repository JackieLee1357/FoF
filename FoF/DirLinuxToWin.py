#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: DirLinuxToWin.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 12月 21, 2021
# ---
import os
import paramiko

hostname = '10.127.3.133'
username = 'yuan_li5928@jabil.com'
password = '`123QWERasdf'
port = 22


def remote_scp(remote_path, local_path):
    t = paramiko.Transport((hostname, 22))
    t.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(t)
    src = remote_path
    des = local_path
    sftp.get(src, des)
    t.close()


if __name__ == '__main__':
    remote_path = '/data/airflow/dags/Kafka/faildata/'
    local_path = 'D:/test/faildata/'
    if not os.path.exists(local_path):
        os.mkdir(local_path)
    remote_scp(remote_path, local_path)
