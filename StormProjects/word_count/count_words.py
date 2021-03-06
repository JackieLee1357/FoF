#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: count_words.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 12月 07, 2021
# ---


from collections import defaultdict
from collections import namedtuple
import logging

from pyleus.storm import SimpleBolt

log = logging.getLogger('counter')

Counter = namedtuple("Counter", "word count")


class CountWordsBolt(SimpleBolt):

    OUTPUT_FIELDS = Counter

    def initialize(self):
        self.words = defaultdict(int)

    def process_tuple(self, tup):
        word, = tup.values
        self.words[word] += 1
        log.debug("{0} {1}".format(word, self.words[word]))
        self.emit((word, self.words[word]), anchors=[tup])


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/word_count_count_words.log',
        format="%(message)s",
        filemode='a',
    )

    CountWordsBolt().run()