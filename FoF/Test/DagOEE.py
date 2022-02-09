#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: DagOEE.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 12月 01, 2021
# ---


from __future__ import print_function
import datetime
from Test import sendMail
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "airflow",  # 用户名的任务的所有者
    "depends_on_past": False,  # 当设置为true时，任务实例将依次运行，同时依赖上一个任务的计划成功。允许start_date的任务实例运行。
    "start_date": datetime.datetime.now().replace(minute=0, second=60, microsecond=0) + datetime.timedelta(hours=-2),
    # 开始时间
    "email": ["yuan_li5928@jabil.com"],
    "email_on_failure": True,  # 任务失败发邮件
    "email_on_retry": False,  # 邮件重发
    "retries": 10,  # 重试策略 (这里是重试一次)
    "retry_delay": datetime.timedelta(seconds=60),  # 每120秒重试一次
    "dag_concurrency": 32,  # 调度器允许并发运行的任务实例的数量
    "max_active_runs_per_dag": 1,  # 每个DAG的最大活动DAG运行次数
    # 'queue': 'bash_queue',#运行此作业时要定位到哪个队列
    'pool': 'OEEPool',
    'priority_weight': 100,
    # 'end_date': datetime(2016, 1, 1),
}

# schedule_interval 是要自己设置的时间格式在底下 cron
dag = DAG("OEE", default_args=default_args, schedule_interval=datetime.timedelta(hours=1))

t1 = BashOperator(
    task_id="RunAssyOEE",  # 任务的唯一，有意义的id
    bash_command='python3 /usr/local/airflow/dags/Assy/AssyOEE.py ',  # 要执行的命令，命令集或对bash脚本（必须为'.sh'）
    dag=dag)

t2 = BashOperator(
    task_id="RunCNCOEE",  # 任务的唯一，有意义的id
    bash_command='python3 /usr/local/airflow/dags/CNC/CNCOEE.py ',  # 要执行的命令，命令集或对bash脚本（必须为'.sh'）
    dag=dag)

t3 = BashOperator(
    task_id="RunPVDOEE",  # 任务的唯一，有意义的id
    bash_command='python3 /usr/local/airflow/dags/PVD/PVDOEE.py ',  # 要执行的命令，命令集或对bash脚本（必须为'.sh'）
    dag=dag)

t4 = BashOperator(
    task_id="RunRobotOEE",  # 任务的唯一，有意义的id
    bash_command='python3 /usr/local/airflow/dags/Robot/RobotOEE.py ',  # 要执行的命令，命令集或对bash脚本（必须为'.sh'）
    dag=dag)

t5 = BashOperator(
    task_id="RunRobotPara",  # 任务的唯一，有意义的id
    bash_command='python3 /usr/local/airflow/dags/Robot/RobotPara.py ',  # 要执行的命令，命令集或对bash脚本（必须为'.sh'）
    dag=dag)


def sendmail(args):
    sender = "OE-FoF_Airflow@jabil.com"
    receiver = ['yuan_li5928@jabil.com']  # 收件人，多个要传list ['a @ qq.com','b @ qq.com]
    cc = ['yuan_li5928@jabil.com']  # cc人员
    subject = 'OEE程式Airflow运行报告'  # 邮件标题
    content = 'AllTaskSuccess'  # 邮件正文
    image = None  # 图片路径（绝对路径）
    file = None  # 文件路径（绝对路径）
    mail = sendMail.SendMail(content=content, sender=sender, receiver=receiver, cc=cc, subject=subject, image=image, file=file)
    mail.send_mail()  # 发邮件


t6 = PythonOperator(
    task_id='AllTaskSuccess',
    provide_context=True,
    python_callable=sendmail,
    dag=dag)

t4 >> t5
t6 << t1
t6 << t2
t6 << t3
t6 << t5
