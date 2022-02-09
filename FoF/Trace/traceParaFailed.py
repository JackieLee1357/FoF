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
import datetime
import pandas as pd
import Trace.dfToClk as dk

# pd.set_option('display.max_columns', None)  # 显示所有列


def getFailData(database, tableName, startTime):
    querySql = f"""SELECT id, sn, process , sntype , agent, testresult , lineid , createtime, fixtureid , 
        logevent , stationid , defects , uut_stop, uut_start, test_result , unit_serial_number
        FROM {database}.TracePara1Day a inner join (SELECT band,bg,fg,sp FROM {database}.TracePara1Day
        prewhere (logevent == 'fail' or logevent == 'scrap' or testresult == 'fail') 
        AND createtime>toDateTime('{startTime}')) b 
        on (a.sn=b.band or a.sn=b.bg or a.sn=b.sp or a.sn=b.fg) """
    querySql2 = f"""select id from {database}.{tableName} where createtime>addHours(now(), -6);"""  # 查询分表已导入数据
    # print(querySql)
    res = dk.fromClick(querySql)
    df = pd.DataFrame(res)
    df = df.drop_duplicates(subset=[0])  # 去重：删除所有重复值
    res2 = dk.fromClick(querySql2)
    df2 = pd.DataFrame(res2)  # 转为df
    df = df.append(df2)
    df = df.append(df2)
    df = df.drop_duplicates(subset=[0], keep=False)  # 去重：删除所有重复值
    print(f"获取{len(df)}条数据成功~")
    if df is None or len(df) == 0:
        print("无可更新数据~")
        return
    columns = ['id', 'sn', 'process', 'sntype', 'agent', 'testresult', 'lineid', 'createtime', 'fixtureid', 'logevent',
               'stationid', 'defects', 'uut_stop', 'uut_start', 'test_result', 'unit_serial_number']
    df.columns = columns
    df = df.sort_values(['sn', 'createtime'])
    df = df.reset_index(drop=True)
    ids = "'" + "','".join(df['id']) + "'"
    querySql3 = f"""SELECT id, results FROM {database}.TraceResult1Day prewhere id in({ids});"""  # 查询一小时内的数据
    res3 = dk.fromClick(querySql3)
    df3 = pd.DataFrame(res3)
    if len(df3) > 0:
        df3 = df3.drop_duplicates(subset=[0])  # 去重：删除所有重复值
        df3.columns = ['id', 'results']
        df = pd.merge(df, df3, how='left', on='id')
    # print(snDf)
    # dk.toClick(snDf, dbTable)
    df['project'] = database[5:]
    print(f"获取{len(df)}条数据成功，准备插入数据库~")
    dbTable = database + '.' + tableName
    dk.toClick(df, dbTable)
    print("插入数据成功~")


# if __name__ == '__main__':
def RunTraceParaFailed(project):
    # project = 'D49'
    table = 'TraceEventFailed'
    database = 'JGPWM' + project
    minutes = -61  # 获取数据的时间-1为1小时前
    startTime = (datetime.datetime.now() + datetime.timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"插入{startTime}时段数据：")
    getFailData(database, table, startTime)



