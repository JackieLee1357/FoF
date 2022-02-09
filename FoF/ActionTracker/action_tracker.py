#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: actionTracker2.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site:
# @Time: 8月 21, 2021
# ---

import base64
import configparser
import datetime
import os
import smtplib
import sys
import traceback

import numpy as np
import pandas as pd
import pymssql
import sqlalchemy
import matplotlib
import sqlalchemy.sql.default_comparator
from matplotlib import pyplot as plt, ticker
from email.header import Header
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from importlib import reload
from builtins import range
from pylab import *

stdi, stdo, stde = sys.stdin, sys.stdout, sys.stderr
reload(sys)  # 通过import引用进来时,setdefaultencoding函数在被系统调用后被删除了，所以必须reload一次
sys.stdin, sys.stdout, sys.stderr = stdi, stdo, stde


def read_data_source(path_name, date_day, date_now0, project0):
    """
    :param path_name:
    :param date_day:
    :param date_now0:
    :param project0:
    :return:
    """
    # 读取数据源excel，进行数据处理，并返回DataFrame
    sheet_keys = list(pd.read_excel(path_name, sheet_name=None))  # 读取excel
    pd.set_option('display.max_columns', None)  # 显示所有列
    pd.set_option('display.max_rows', None)  # 显示所有行
    sheet_datas = pd.DataFrame()
    for sheet in sheet_keys:
        sheet_data = pd.read_excel(path_name, sheet_name=sheet_keys.index(sheet))
        sheet_datas = pd.concat([sheet_datas, sheet_data])
    print('-' * 20)

    pivot_data = pd.pivot_table(sheet_datas, values=['serials'], index=['process', 'err_process'], columns=['build'],
                                aggfunc=len, fill_value=0)  # 创建透视表，aggfunc=len：对value计数
    pivot_data = pivot_data.reset_index(drop=False)
    columns = [''.join(i).replace('serials', '') for i in pivot_data.columns]
    pivot_data.columns = columns
    pivot_data['日期'] = date_now0
    pivot_data['project'] = project0
    print('源数据读取完毕')
    print('------------')
    return pivot_data
