#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: auditDDS.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 10月 19, 2021
# ---
import datetime
import os
import sys
import pandas
import sqlalchemy


def readDataFromExcel(pathName):
    # 读取数据源excel，进行数据处理，并返回DataFrame
    # pandas.set_option('display.max_columns', None)  # 打印数据时显示所有列
    # pandas.set_option('display.max_rows', None)  # 打印数据时显示所有行
    sheetKeys = pandas.read_excel(pathName, sheet_name=None)  # 读取excel所有sheet
    sheetData = pandas.read_excel(pathName, sheet_name=list(sheetKeys)[0])  # 读取excel里sheet数据
    sheetData = sheetData.iloc[1:, 0:]
    sheetData['createtime'] = datetime.datetime.now()
    sheetData['createuser'] = 'OE-FoF User'
    sheetData['updatetime'] = datetime.datetime.now()
    sheetData['updateuser'] = 'OE-FoF User'
    sheetData['ipqano'] = 'IPQA' + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    # sql_columns = ["ipqano", "findingno", "site", "station_number", "process", "product_process_characteristics",
    #                "audit_items", "risk_level", "non_conformance", "pic", "pic_supervisor", "qe_approval",
    #                "wcm_approval",
    #                "qa_manager", "department", "issue", "issue_date", "remark", "rootcause", "containmentaction",
    #                "correctiveaction", "targetdate", "personincharge", "preventiveaction", "preventivedate",
    #                "papic", "ipqa_status", "rejectcount", "rejectcause", "mistakeproofing", "updateuser", "updatetime",
    #                "createuser", "createtime", "project", "audit_level", "audit_category", "lead_for_audit",
    #                "improve_status", "risk_determination"]
    data = sheetData[~sheetData['findingno'].isin(dataInSql)]
    finalData = sheetData.loc[data.index]
    # print('--------')
    # print(finalData)
    print('源数据读取完毕')
    print('------------')
    return finalData


def insertIntoPGSQL(frame, tableName1):
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    frame.to_sql(tableName1, engine, index=False, if_exists='append')
    engine.dispose()
    print('数据写入SQL Server成功')
    print('------------')


def configFromPGSQL(tableName2):  # 链接PG数据库，查询已处理的时段
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute(f'select distinct findingno from {tableName2} order by findingno desc')  #
    row = cursor.fetchall()
    # row = [i[0].strftime("%Y-%m-%d %H") + ':00:00' for i in row]  # 时间转化为字符串
    row = [''.join(a) for a in row]  # 元组转化为列表
    connection.commit()
    cursor.close()
    engine.dispose()
    return row


if __name__ == '__main__':
    rootPath = os.path.abspath(os.path.dirname(sys.argv[0]))  # 获取当前执行文件夹路径
    excelPath = rootPath + "/9.28 E-Audit upgrade to CI program Schedule.xlsx"
    tableName = 'ipqa_details'
    dataInSql = configFromPGSQL(tableName)
    rootData = readDataFromExcel(excelPath)
    insertIntoPGSQL(rootData, tableName)
