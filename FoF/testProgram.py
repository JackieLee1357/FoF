#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: testProgram.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 1月 21, 2022
# ---

import cProfile
import timeit
from Trace.tracePara import RunTracePara

if __name__ == '__main__':
    p = cProfile.Profile()
    p.enable()
    # 输入需要测试的方法
    RunTracePara('D49')
    p.disable()
    p.print_stats(sort='tottime')


