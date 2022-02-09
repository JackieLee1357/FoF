#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: DagTraceKafka.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 12月 20, 2021
# ---

from __future__ import print_function
import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from TraceMQ.Kafka.traceMqConsumer import traceMqConsumer

default_args = {
    "owner": "airflow",  # 用户名的任务的所有者
    "depends_on_past": False,  # 当设置为true时，任务实例将依次运行，同时依赖上一个任务的计划成功。允许start_date的任务实例运行。
    "start_date": datetime.datetime.now().replace(minute=0, second=50, microsecond=0) + datetime.timedelta(minutes=-1),
    # 开始时间
    "email": ["yuan_li5928@jabil.com"],
    "email_on_failure": True,  # 任务失败发邮件
    "email_on_retry": False,  # 邮件重发
    "retries": 1,  # 重试策略 (这里是重试一次)
    "retry_delay": datetime.timedelta(seconds=60),  # 每60秒重试一次
    "dag_concurrency": 32,  # 调度器允许并发运行的任务实例的数量
    "max_active_runs_per_dag": 1,  # 每个DAG的最大活动DAG运行次数
    # 'queue': 'bash_queue',#运行此作业时要定位到哪个队列
    'pool': 'TracePool',
    'priority_weight': 110,
    'dagrun_timeout_sec': 1180,  # 每次任务最长执行时间
    'killed_task_cleanup_time': 1180  # 接受信号超时时间
    # 'end_date': datetime(2016, 1, 1),
}

# schedule_interval 是要自己设置的时间格式在底下 cron
dag = DAG("TraceKafka", default_args=default_args, schedule_interval=datetime.timedelta(minutes=20))


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


t1 = PythonOperator(
    task_id='runConsumerResult',
    provide_context=True,
    python_callable=runConsumerResult,
    dag=dag)

t2 = PythonOperator(
    task_id='runConsumerLog',
    provide_context=True,
    python_callable=runConsumerLog,
    dag=dag)

t3 = PythonOperator(
    task_id='runConsumerHistory',
    provide_context=True,
    python_callable=runConsumerHistory,
    dag=dag)



