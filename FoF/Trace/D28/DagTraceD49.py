#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: DagRobotOEE.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 11月 27, 2021
# ---

from __future__ import print_function
import datetime
from airflow.models import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from Trace.tracePara import RunTracePara
from Trace.traceResultFailed import RunTraceResultFailed
from Trace.traceParaFailed import RunTraceParaFailed

default_args = {
    "owner": "airflow",  # 用户名的任务的所有者
    "depends_on_past": False,  # 当设置为true时，任务实例将依次运行，同时依赖上一个任务的计划成功。允许start_date的任务实例运行。
    "start_date": datetime.datetime.now().replace(minute=0, second=20, microsecond=0) + datetime.timedelta(hours=-1),
    # 开始时间
    "email": ["yuan_li5928@jabil.com"],
    "email_on_failure": False,  # 任务失败发邮件
    "email_on_retry": False,  # 邮件重发
    "retries": 5,  # 重试策略 (这里是重试一次)
    "retry_delay": datetime.timedelta(seconds=30),  # 每120秒重试一次
    "dag_concurrency": 32,  # 调度器允许并发运行的任务实例的数量
    "max_active_runs_per_dag": 1,  # 每个DAG的最大活动DAG运行次数
    # 'queue': 'bash_queue',#运行此作业时要定位到哪个队列
    'pool': 'TracePool',
    'priority_weight': 100,
    'dagrun_timeout_sec': 1200,  # 每次任务最长执行时间
    # 'end_date': datetime(2016, 1, 1),
}
# schedule_interval 是要自己设置的时间格式在底下 cron
project = "D49"
dag = DAG(f"Trace{project}", default_args=default_args, schedule_interval=datetime.timedelta(minutes=20))


def runTracePara(**kwargs):
    RunTracePara(project)


def runTraceResultFailed(**kwargs):
    RunTraceResultFailed(project)


def runTraceParaFailed(**kwargs):
    RunTraceParaFailed(project)


t1 = PythonOperator(
    task_id=f'{project}RunTracePara',
    provide_context=True,
    python_callable=runTracePara,
    dag=dag)

t2 = PythonOperator(
    task_id=f'{project}RunTraceParaFailed',
    provide_context=True,
    python_callable=runTraceParaFailed,
    dag=dag)

t3 = PythonOperator(
    task_id=f'{project}RunTraceResultFailed',
    provide_context=True,
    python_callable=runTraceResultFailed,
    dag=dag)

t1 >> t2 >> t3

OneFailed = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED,
    task_id=f'{project}TraceFailed',
    to=["yuan_li5928@jabil.com"],
    subject=f"{project}Trace Failed",
    html_content=f'<h3>{project}Trace Failed</h3>')

OneFailed.set_upstream([t1, t2, t3])
