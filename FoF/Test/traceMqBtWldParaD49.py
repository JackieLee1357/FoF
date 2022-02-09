#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: traceMqBtWldParaD49.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 1月 03, 2022
# ---


import sys
import pandas as pd
import dfToClk as dk

pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.max_rows', None)  # 显示所有行


def insUmpToClk(pjt, pcs, db):
    querySql = f"""SELECT *
            FROM JGPWM.view_TraceMQParameter WHERE process='{pcs}' and project='{pjt}';"""  # 查询uvpu D49数据
    try:
        ump1D49SqlData = dk.fromClick(querySql)
        ump1D49RawData = pd.DataFrame(ump1D49SqlData)  # 转为df
        print("从数据库获取数据成功~")
    except Exception as e:
        print("从数据库查询数据失败，报错代码为：" + str(e))
        sys.exit(1)
    columns = ['id', 'project', 'process', 'sn', 'sntype', 'agent', 'testresult', 'lineid', 'starttime', 'endtime',
               'fixtureid',
               'logcreated', 'hiscreated', 'logevent', 'hisevent', 'stationid', 'serials', 'defects', 'data', 'results']
    ump1D49RawData.columns = columns
    ump1D49RawData = ump1D49RawData.drop(columns='results')
    # ump1D49RawData = ump1D49RawData.reset_index(drop=True)  # 重置索引
    umpPd = ump1D49RawData
    umpPd = umpPd.reset_index(drop=True)  # 重置索引
    data = umpPd['data'].apply(lambda x: eval(x).values()).explode().apply(pd.Series)  # 展开data列
    umpPd = umpPd.join(data).drop(columns='data')
    data = umpPd['serials'].apply(lambda x: eval(x)).apply(pd.Series)  # 展开serials列
    umpPd = umpPd.join(data).drop(columns='serials')
    print(umpPd.columns)
    dataCol = ['uut_attributes', 'test_attributes', 'test_station_attributes']  #
    for col in dataCol:  # 解开json
        dataColumn = umpPd[col].apply(pd.Series)
        umpPd = umpPd.join(dataColumn).drop(columns=col)
        # print(umpPd.columns)
    # print(umpPd)
    print(umpPd.columns)
    try:
        dk.toClick(umpPd, db)  # df插入数据库
        print("写入数据库成功~")
    except Exception as e:
        print("插入数据库失败，报错代码为：" + str(e))
        sys.exit(1)


if __name__ == '__main__':
    project = "D49"
    process = "bt-wld"
    tableName = "TraceMQParaBtWld"
    print(f"插入{project}专案数据：")
    insUmpToClk(project, process, tableName)