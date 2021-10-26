"""
Version: Python3.5
Author: OniOn
Site: http://www.cnblogs.com/TM0831/
Time: 2018/12/27 14:49

读取文件夹文件内容，提取需要的文本，写入新的文本文件
"""
import os
from win32com.client import Dispatch, constants, gencache, DispatchEx


class Tool:
    def __init__(self):
        self.path = "D:\工作文档\SOP文档\IT- SOP\\"
        self.path_pdf = "D:\工作文档\SOP文档\IT- SOP\pdf文件\\"

    def eachFile(self):
        fileNameList = []
        pathDir = os.listdir(self.path)

        for allDir in pathDir:

            # 判断是否是文件
            if os.path.isfile(os.path.join(self.path, allDir)):
                child = os.path.join('%s%s' % (self.path, allDir))
                # print(child)
                fileNameList.append(child)
        return fileNameList

    def test(self, fillName):
        #  读取文本文件，进行读写操作
        file_object = open(fillName)
        # a --追加模式  w
        file_write = open("D:/test.txt", 'a')
        try:
            line = file_object.readline()
            while line:
                array = line.split(":")
                if len(array) > 4:
                    # print(array)
                    # print(array[0])
                    date = str(array[0] + ":" + array[1] + ":" + array[2].split(" ")[0])
                    # print(date)
                    list1 = array[5].split("=")
                    # print(list1[4])
                    # print()
                    file_write.write(date)
                    file_write.write("    ")
                    file_write.write(list1[4])
                line = file_object.readline()

        finally:
            file_object.close()
            file_write.close()

    # 更改文件名称
    def add_text_nane(self,file_path):
        #  读取文本名称，进行修改操作
        try:
            file_name = os.path.basename(file_path)
            if (file_name.startswith('IT')):
                print('不用改')
                # 替换字符串中的内容 注意：replace方法不改变原来的值，需要赋值
                file_name = file_name.replace(' ','')
                os.rename(file_path, self.path + file_name)
            else:
                file_newname = 'IT- '+ file_name
                file_newname.replace('top','TOP')
                os.rename(file_path, self.path + file_newname)
        except Exception as e:
            print(e)

    # 创建文件(mknod 报错)
    def creat_file(self,file_path):
        file_path = str(file_path).replace('.docx','.pdf')
        open(file_path,'a')

    # word转pdf
    def wordToPdf(self,file_path):
        file_name = os.path.basename(file_path)
        pdf_name = file_name.replace('.docx','.pdf')
        pdf_path = self.path_pdf + pdf_name
        # 创建文件
        open(pdf_path,'a')
        w = DispatchEx("Word.Application")
        try:
            # 打开文件
            doc = w.Documents.Open(file_path, ReadOnly = 1)
            # 转换文件
            doc.SaveAs(pdf_path, FileFormat = 17)
            doc.close()
        except Exception as e:
            print("转换失败，失败原因：" +str(e))


if __name__ == '__main__':
    dp = Tool()
    file_path_list = dp.eachFile()
    for file_path in file_path_list:
        # print(file_path)
        if (file_path.endswith('.docx')):
            print(file_path)
            dp.wordToPdf(file_path)
        # dp.add_text_nane(file_path)
        # dp.test(fileName)
