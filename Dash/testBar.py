#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: testBar.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 8月 16, 2021
# ---


import numpy as np
import matplotlib.pyplot as plt

# 构造数据
from plotly.tools import mpl_to_plotly

N = 10
ind = np.arange(N)
bars = np.random.randn(N)
t = np.arange(0.01, 10.0, 0.01)

# 新建左侧纵坐标画板
fig, ax1 = plt.subplots()
# 画柱状图
ax1.bar(ind, bars, alpha=0.3)
ax1.set_xlabel('$x$')
# 显示左侧纵坐标
ax1.set_ylabel('bar', color='b')
[tl.set_color('b') for tl in ax1.get_yticklabels()]

# 新建右侧纵坐标画板
ax2 = ax1.twinx()
# 画曲线
ax2.plot(t, np.sin(0.25 * np.pi * t), 'r-')
# 显示右侧纵坐标
ax2.set_ylabel('sin', color='r')
[tl.set_color('r') for tl in ax2.get_yticklabels()]

plt.show()

plotly_fig = mpl_to_plotly(fig)
