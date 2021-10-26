#!/C:\Users\Administrator\PycharmProjects\pythonProject\venv\Scripts python3.9
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: getSerialNumber.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 8月 05, 2021
# ---
import time

import pymssql
import pandas
import numpy
import time
import pyodbc
import pymssql._mssql

def readExcel(pathName):
    # 读取数据源excel，进行数据处理，并返回DataFrame
    df = pandas.read_excel(pathName, sheet_name='Sheet1')  # 读取excel
    lists = []
    pandas.set_option('display.max_columns', None)  # 显示所有列
    print(len(df))
    for i in range(len(df)):
        lists.append(str(df.loc[i,'SerialNumber']))
    print('源数据读取完毕')
    print('------------')
    return lists

def getDataFromSqlS(sql):
    try:
        cnxn = pyodbc.connect(r'Driver={SQL Server};Server=cnwxim0msarch01;Database=JEMS;Trusted_Connection=yes;')
        cursor = cnxn.cursor()
        cursor.execute(sql)
        df = pandas.DataFrame(cursor.fetchall())
        cursor.close()
        cnxn.close()
        return df
    except pymssql.Error as e:
        print(e)
        return None


def writeToSheets2(pathName, sheets, dfs):
    # 数据写入excel中对应的sheet
    # 打开excel
    writer = pandas.ExcelWriter(pathName)
    # sheets是要写入的excel工作簿名称列表
    for i in range(len(sheets)):
        dfs[i].to_excel(writer, sheet_name=sheets[i])
        # 保存writer中的数据至excel
    writer.save()
    print('数据已写入EXCEL')
    print('------------')


if __name__ == '__main__':
    path1 = 'C:\getSerialNumber/getSerialNumber.xlsx'
    path2 = 'C:\getSerialNumber/SerialNumber.xlsx'
    sheet = "1"
    dataSource = readExcel(path1)
    i = 0
    while i < len(dataSource):
        dataSource = dataSource[i:i+4000]
        print(dataSource)
        dataSource = "','".join(dataSource)
        dataSource = "'"+dataSource+"'"
        print(dataSource)
        sql = f"""SELECT  cc.containernumber,ww.serialnumber,cwl.routestep_id
          FROM [JEMS].[dbo].[EP_ContainerWipLink] cwl
          inner join CR_Containers cc on cwl.Container_ID=cc.Container_ID
          inner join wp_wip ww on ww.Wip_ID=cwl.Wip_ID
          where ww.serialnumber in({dataSource})"""
        data = getDataFromSqlS(sql)
        writeToSheets2(path2, sheet, data)
        print(f"第{i}条数据已完成~")
        time.sleep(10)
        i += 3999

