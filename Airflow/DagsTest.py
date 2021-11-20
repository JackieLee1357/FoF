#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: DagsTest.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 11月 20, 2021
# ---

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',  # DAG的所有者，会在Web UI上显示，主要用于方便管理
    'depends_on_past': False,  # 是否依赖于过去。如果为True，那么必须要昨天的DAG执行成功了，今天的DAG才能执行。
    'start_date': datetime(2015, 6, 1),  # DAG的开始时间(必须一个datetime对象，不可以用字符串)
    'email': ['airflow@example.com'],  # 出问题时，发送报警Email的地址，可以填多个，用逗号隔开。
    'email_on_failure': False,  # 任务失败且重试次数用完时是否发送Email，推荐填True。
    'email_on_retry': False,  # 任务重试时是否发送Email
    'retries': 1,  # 任务失败后的重试次数
    'retry_delay': timedelta(minutes=5),  # 重试间隔，必须是timedelta对象。
}

# 第一个参数固定为dag的名字,schedule_interval为执行时间间隔
dag = DAG('tutorial', default_args=default_args, schedule_interval=timedelta(hours=1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(  # 任务类型是bash
    task_id='echoDate',  # 任务id
    bash_command='echo date > /home/datefile',  # 任务命令
    dag=dag)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag)

t2.set_upstream(t1)  # 定义任务信赖，任务2依赖于任务1
