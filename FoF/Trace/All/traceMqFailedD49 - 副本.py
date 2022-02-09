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

import pandas as pd
import dfToClk as dk

pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.max_rows', None)  # 显示所有行


def getFailData(hour):
    querySql = f"""SELECT  sn , project , process , sntype , agent, testresult , lineid , starttime, endtime , fixtureid , logcreated,
        hiscreated, logevent , stationid , defects , uut_stop, uut_start, test_result , unit_serial_number , line_id ,
        fixture_id , station_id , software_name , software_version
        FROM JGPWMD49.TraceParaAll a inner join (SELECT distinct band,bg,fg,sp FROM JGPWMD49.TraceParaAll
        WHERE (logevent == 'fail' or logevent == 'scrap' or testresult == 'fail') 
        AND endtime>toStartOfHour(addHours(now(), {hour}))) b 
        on (a.sn=b.band or a.sn=b.bg or a.sn=b.sp or a.sn=b.fg) """
    print(querySql)
    res = dk.fromClick(querySql)
    if res is None or len(res) == 0:
        print("无可更新数据~")
        return
    columns = ['sn', 'project', 'process', 'sntype', 'agent', 'testresult', 'lineid', 'starttime', 'endtime',
               'fixtureid', 'logcreated', 'hiscreated', 'logevent', 'stationid', 'defects', 'uut_stop', 'uut_start',
               'test_result', 'unit_serial_number', 'line_id', 'fixture_id', 'station_id', 'software_name',
               'software_version']
    snDf = pd.DataFrame(res, columns=columns)
    snDf = snDf.sort_values(['sn', 'starttime'])
    snDf = snDf.reset_index(drop=True)
    print(snDf)
    dk.toClick(snDf, 'TraceFailed')


if __name__ == '__main__':
    project = "D49"
    hour = -1  # 获取数据的时间-1为1小时前
    getFailData(hour)
