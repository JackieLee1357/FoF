#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: Dash1.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 10月 15, 2021
# ---

import dash
import dash_core_components as dcc
import dash_html_components as html

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
        figure={
            'data': [
                {'x': [1, 2, 3], 'y': [3, 4, 2], 'type': 'plot', 'name': '数据源A'},
                {'x': [1, 2, 3], 'y': [2, 3, 5], 'type': 'plot', 'name': '数据源B'},
            ],
            'layout': {
                'title': '数据展示'
            }
        }
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)
