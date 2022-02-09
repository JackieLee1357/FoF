#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: testMQ.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 12月 20, 2021
# ---


import json
from ast import literal_eval
import simplejson
import ijson
import os
import pandas as pd

path = "//CNWGPM0PG81/Test/Log/20211205124106.file"

with open(path, encoding='utf-8') as f:
    str = f.read()
    # str2 = "[" + str + "]"
    # print(str2)
    # new_list = eval(str)
    # print(type(new_list))  #
    # objects = json.loads(new_list)
    # objects = ijson.parse(f)
    s2 = pd.read_json(f)
    print(s2)
    # print(new_list)

