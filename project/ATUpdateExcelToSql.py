#!/C:\Users\Administrator\PycharmProjects\pythonProject\venv\Scripts python3.9
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: ppDowntime.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site:
# @Time: 8月 02, 2021
# ---
import time
import pandas
import configparser
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DB_URI = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
engine = create_engine(DB_URI)
Base = declarative_base(engine)
session = sessionmaker(engine)()


class Article(Base):
    __tablename__ = "actiontracker_faca"
    item_id = Column(Integer, primary_key=True, autoincrement=True)
    fa = Column(String(150))
    ca = Column(String(150))
    returndri = Column(String(50))
    returntime = Column(String(50))

    def __str__(self):
        return '[%s, %s, %s, %s, %s]' % (self.item_id, self.fa, self.ca, self.returndri, self.returntime)


def update_data(updateData):
    for i in range(len(updateData)):
        dataup = {Article.fa: updateData.loc[i, 'FA'], Article.ca: updateData.loc[i, 'CA'],
                  Article.returndri: updateData.loc[i, 'returndri'],
                  Article.returntime: updateData.loc[i, 'returntime']}
        article = session.query(Article).filter_by(item_id=str(updateData.loc[i, 'item_id'])).update(dataup)
    session.commit()
    print('数据更新成功')


def readExcel(pathName):
    # 读取数据源excel，进行数据处理，并返回DataFrame
    sheetKeys = pandas.read_excel(pathName, sheet_name=None)  # 读取excel
    pandas.set_option('display.max_columns', None)  # 显示所有列
    # print(list(sheetKeys))   #显示所有sheet名称
    sheetFACA = pandas.read_excel(pathName, sheet_name=list(sheetKeys).index(list(sheetKeys)[0]))
    sheetFACA.drop(sheetFACA.columns[0], axis=1, inplace=True)
    print('源数据读取完毕')
    print('------------')
    return sheetFACA


def getUpdateData(frames):
    frames = frames.fillna(value='@@@')
    isNulList = frames[~frames['FA'].isin(['@@@'])&~frames['FA'].isin(['待回复'])]        # isin按照列内容自动筛选
    isNulList.reset_index(drop=True, inplace=True)  # 行号重新排序
    frames = isNulList.loc[0:, ['item_id', 'FA', 'CA', 'returndri', 'returntime']]
    print('数据获取成功')
    print('--------')
    return frames


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('C:\ActionTracker\FACA.ini', encoding="utf-8")  # 导出配置文件
    outputPath = config.get("messages", "outputPath")       # 处理后文件网址
    SQLTableName = ['actiontracker_faca', 'actiontracker_tracemissing']
    excelData = readExcel(outputPath)
    updateData = getUpdateData(excelData)
    print("更新数据为：")
    print(updateData)
    update_data(updateData)
    time.sleep(5)  # 暂停5秒
