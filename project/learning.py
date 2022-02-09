#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: learning.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 9月 27, 2021
# ---


import util
times = 1
print('你好，我是聊天机器人-小象宝宝。\n有人说我有10岁人类的智商，你想试试吗？\n我可以回答你3个问题，来吧。')
while True:
    me = input('第{}个问题：'.format(times))
    print('小象宝宝：' + util.talk(me))
    if me == '再见' or times >= 3:
        print('小象宝宝：我要走了，祝你学得快乐，再见！')
        break
    times += 1



import recognition
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

client = recognition.recog()
id = 'https://cdn.xiaoxiangxueyuan.com/pimages/material/20200113/c0ff24a1a2081faaff6fb4feeebf5c88.png'
image = requests.get(id).content
res = client.basicGeneral(image)
for i in range(len(res['words_result'])):
    print(res['words_result'][i]['words'])