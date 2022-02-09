#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: dummy_bolt.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 12月 07, 2021
# ---


from pyleus.storm import SimpleBolt


class DummyBolt(SimpleBolt):
    OUTPUT_FIELDS = ['sentence']

    def process_tuple(self, tup):
        sentence, name = tup.values
        new_sentence = "{0} says, \"{1}\"".format(name, sentence)
        self.emit((new_sentence,), anchors=[tup])


if __name__ == '__main__':
    DummyBolt().run()
