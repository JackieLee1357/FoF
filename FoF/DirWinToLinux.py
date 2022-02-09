#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: test1.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site:
# @Time: 12月 21, 2021
# ---

import paramiko
import datetime
import os

hostname = '10.127.3.133'
username = 'yuan_li5928@jabil.com'
password = '`123QWERasdf'
port = 22


def upload(local_dir, remote_dir):
    try:
        t = paramiko.Transport((hostname, port))
        t.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(t)
        print('开始传输文件： %s ' % datetime.datetime.now())
        for root, dirs, files in os.walk(local_dir):
            print('本地文件夹内容：路径：[%s] 文件夹：[%s] 文件：[%s]' % (root, dirs, files))
            for filespath in files:
                local_file = os.path.join(root, filespath)
                print(11, '文件初始位置：[%s]' % (local_file))
                a = local_file.replace(local_dir, '', 1).replace('\\', '/').lstrip('/')
                remote_file = os.path.join(remote_dir, a)
                print(22, '文件目标位置：[%s]' % remote_file)
                try:
                    sftp.put(local_file, remote_file)
                except Exception as e:
                    # b = os.path.split(remote_file)[0]
                    sftp.mkdir(os.path.split(remote_file)[0])
                    sftp.put(local_file, remote_file)
                    print("66 文件初始位置：%s 文件目标位置：%s" % (local_file, remote_file))
            for name in dirs:
                local_path = os.path.join(root, name)
                print(0, '文件初始位置：[%s]' % local_path)
                a = local_path.replace(local_dir, '', 1).replace('\\', '')
                print(1, '文件目标位置：[%s]' % remote_dir)
                remote_path = os.path.join(remote_dir, a)
                print(33, '文件目标位置：[%s]' % remote_path)
                try:
                    sftp.mkdir(remote_path)
                    print(44, "文件目标位置:%s" % remote_path)
                except Exception as e:
                    print(55, e)
        print('88, 文件传输成功：%s ' % datetime.datetime.now())
        t.close()
    except Exception as e:
        print(88, e)


if __name__ == '__main__':
    local_dir = r'Trace/D28'
    remote_dir = r'/data/airflow/dags/Trace/'  # + local_dir + '/'
    upload(local_dir, remote_dir)
