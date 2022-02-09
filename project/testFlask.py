#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: testFlask.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 8月 18, 2021
# ---


from flask import Flask, Markup
from flask import render_template

app = Flask(__name__)


@app.route('/hello')
@app.route('/hello/<name>')
@app.route('/')
def index():
    return Markup('<div>Hello %s</div>') % '<em>Flask</em>'


def hello(name=None):
    return render_template('hello.html', name=name)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
