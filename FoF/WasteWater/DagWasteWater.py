#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: DagBandOEE.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 11月 27, 2021
# ---

from __future__ import print_function
import datetime
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "airflow",  # 用户名的任务的所有者
    "depends_on_past": False,  # 当设置为true时，任务实例将依次运行，同时依赖上一个任务的计划成功。允许start_date的任务实例运行。
    "start_date": datetime.datetime.now().replace(minute=3, second=30, microsecond=0) + datetime.timedelta(hours=-1),
    # 开始时间
    "email": ["yuan_li5928@jabil.com"],
    "email_on_failure": True,  # 任务失败发邮件
    "email_on_retry": False,  # 邮件重发
    "retries": 1,  # 重试策略 (这里是重试一次)
    "retry_delay": datetime.timedelta(seconds=60),  # 每120秒重试一次
    "dag_concurrency": 32,  # 调度器允许并发运行的任务实例的数量
    "max_active_runs_per_dag": 1,  # 每个DAG的最大活动DAG运行次数
    # 'queue': 'bash_queue',#运行此作业时要定位到哪个队列
    'pool': 'OtherPool',
    'priority_weight': 110,
    # 'end_date': datetime(2016, 1, 1),
}
# schedule_interval 是要自己设置的时间格式在底下 cron

dag = DAG("WasteWater", default_args=default_args, schedule_interval=datetime.timedelta(hours=1))
t1 = BashOperator(
    task_id="RunWasteWater",  # 任务的唯一，有意义的id
    bash_command='python3 /usr/local/airflow/dags/WasteWater/wasteWater.py',  # 要执行的命令，命令集或对bash脚本（必须为'.sh'）
    dag=dag)

t1Failed = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED,
    task_id="Run_WasteWater_Failed",
    to=["yuan_li5928@jabil.com"],
    subject="Run WasteWater Failed",
    html_content='<h3>Run WasteWater Failed, Please Check!</h3>')

t1 >> t1Failed

