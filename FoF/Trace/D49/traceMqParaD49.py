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

pd.set_option('display.max_columns', None)  # 显示所有列


def getFailData(database, tableName, startTime):
    querySql = f"""SELECT id, sn , project , process , sntype , agent, testresult , lineid , createtime, starttime, endtime , fixtureid , logcreated,
        hiscreated, logevent , stationid , defects , uut_stop, uut_start, test_result , unit_serial_number , line_id ,
        fixture_id , station_id , software_name , software_version
        FROM {database}.TraceParaAll a inner join (SELECT distinct band,bg,fg,sp FROM {database}.TraceParaAll
        prewhere (logevent == 'fail' or logevent == 'scrap' or testresult == 'fail') 
        AND createtime>toDateTime('{startTime}')) b 
        on (a.sn=b.band or a.sn=b.bg or a.sn=b.sp or a.sn=b.fg) """
    querySql2 = f"""select id from {tableName} where createtime>now()-interval '6 hour';"""  # 查询分表已导入数据
    # print(querySql)
    res = dk.fromClick(querySql)
    df = pd.DataFrame(res)
    res2 = dk.fromPG(querySql2)
    df2 = pd.DataFrame(res2)  # 转为df
    df = df.append(df2)
    df = df.append(df2)
    df = df.drop_duplicates(subset=[0], keep=False)  # 去重：删除所有重复值
    print("获取数据成功~")
    if df is None or len(df) == 0:
        print("无可更新数据~")
        return
    columns = ['id', 'sn', 'project', 'process', 'sntype', 'agent', 'testresult', 'lineid', 'createtime', 'starttime',
               'endtime',
               'fixtureid', 'logcreated', 'hiscreated', 'logevent', 'stationid', 'defects', 'uut_stop', 'uut_start',
               'test_result', 'unit_serial_number', 'line_id', 'fixture_id', 'station_id', 'software_name',
               'software_version']
    df.columns = columns
    snDf = df.sort_values(['sn', 'starttime'])
    snDf = snDf.reset_index(drop=True)
    # print(snDf)
    # dk.toClick(snDf, dbTable)
    dk.toPG(snDf, tableName)
    print(f"插入{len(snDf)}数据成功~")


# if __name__ == '__main__':
def RunTraceParaFailed(project):
    # project = 'D63'
    table = 'traceparafailed'
    database = 'JGPWM' + project
    minutes = -40  # 获取数据的时间-1为1小时前
    startTime = (datetime.datetime.now() + datetime.timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"插入{startTime}时段数据：")
    getFailData(database, table, startTime)
