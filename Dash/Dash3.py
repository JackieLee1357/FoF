#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: Dash3.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 10月 15, 2021
# ---

import dash
import dash_core_components as dcc
import dash_html_components as html
import numpy as np
import matplotlib.pyplot as plt
from plotly.tools import mpl_to_plotly


def fig():
    # 构造数据
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
    # plt.show()
    plotly_fig = mpl_to_plotly(fig)
    return plotly_fig


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.H1(children='Dash Demo', style={"text-align": "center"}),

    html.Div(children='''
        一款牛逼的Python开发的应用程序---------Dash
    ''',
             style={"text-align": "center", "color": "red"}),

    dcc.Graph(
        id='example-graph',
        figure=fig()
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)
