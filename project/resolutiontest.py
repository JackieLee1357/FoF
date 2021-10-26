"""
Version: Python3.5
Author: OniOn
Site: http://www.cnblogs.com/TM0831/
Time: 2018/12/27 14:49

读取文件夹文件内容，提取需要的文本，写入新的文本文件
"""
import csv
import os


class Tool:
    def __init__(self):
        self.read_file = "D:/fille/pis_wxi_2020_4_12.csv"
        self.write_file = "D:/fille/pis_wxi_2020_4_11.txt"

    def test(self):
        #  读取文本文件，进行读写操作
        file_object = open(self.read_file)
        # a --追加模式  w
        file_write = open(self.write_file, 'a')
        try:
            line = csv.reader(file_object)
            for row in line:
                row_last = ""
                for rowe in row:
                    rowe.replace('"','')
                    print(rowe)
                    # file_write.write(rowe)
        finally:
            file_object.close()
            file_write.close()


if __name__ == '__main__':
    dp = Tool()
    dp.test()
