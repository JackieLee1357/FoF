#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: split_words.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 12月 07, 2021
# ---


import logging

from pyleus.storm import SimpleBolt

log = logging.getLogger('splitter')


class SplitWordsBolt(SimpleBolt):

    OUTPUT_FIELDS = ["word"]

    def process_tuple(self, tup):
        line, = tup.values
        log.debug(line)
        for word in line.split():
            log.debug(word)
            self.emit((word,), anchors=[tup])


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/word_count_split_words.log',
        format="%(message)s",
        filemode='a',
    )
    SplitWordsBolt().run()