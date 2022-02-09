#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: traceBin.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 1月 15, 2022
# ---

import pandas as pd
import Trace.dfToClk as dk

# pd.set_option('display.max_columns', None)  # 显示所有列


def insBinToClk(database, tableNm, hour):
    querySql = f"""SELECT lineid as line,stationid as station,RECIPE as color,SIM_PROFILEID as bin,count(sn) AS qty,
     toStartOfHour(createtime) as hour,logevent
                FROM {database}.TracePara1Day PREWHERE process='ump-t1'
                and createtime>=toStartOfHour(addHours(now(), {hour}))
                and createtime<toStartOfHour(now())
                GROUP BY RECIPE,lineid, stationid, SIM_PROFILEID, toStartOfHour(createtime), logevent
                ORDER BY hour DESC;"""
    querySql2 = f"""select distinct hour from {tableNm} where hour>now()-interval '{-hour + 2} hour';"""  # 查询分表已导入数据
    res = dk.fromClick(querySql)
    df = pd.DataFrame(res)
    if df is None or len(df) == 0:
        print("无可更新数据~")
        return
    res2 = dk.conHourFromPG(querySql2)
    df2 = pd.DataFrame(res2)  # 转为df
    for hour in df[5].drop_duplicates():
        if df2 is None or len(df2) == 0:
            break
        if hour.strftime("%Y-%m-%d %H:%M:%S") in list(df2[0]):
            df = df[df[5] != hour]
    print("获取数据成功~")
    if df is None or len(df) == 0:
        print("无可更新数据~")
        return
    columns = ['line', 'station', 'color', 'bin', 'qty', 'hour', 'event']
    df.columns = columns
    df = df.reset_index(drop=True)
    df['project'] = database[5:]
    dk.toPG(df, 'tracesimbin')
    print(f"插入{len(df)}数据成功~")


# if __name__ == '__main__':
def RunTraceBin(project):
    # project = 'D49'
    database = 'JGPWM' + project
    hours = -6
    table = 'tracesimbin'
    print("-" * 20)
    print(f"插入{hours}小时到现在时段数据：")
    insBinToClk(database, table, hours)
