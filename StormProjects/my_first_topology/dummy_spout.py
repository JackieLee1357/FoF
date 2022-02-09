#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: dummy_spout.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 12月 07, 2021
# ---


from pyleus.storm import Spout


class DummySpout(Spout):
    OUTPUT_FIELDS = ['sentence', 'name']

    def next_tuple(self):
        self.emit(("This is a sentence.", "spout",))


if __name__ == '__main__':
    DummySpout().run()
