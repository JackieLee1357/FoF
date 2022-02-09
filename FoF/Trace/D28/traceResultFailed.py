#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: traceMqUmpT1ResultD49.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site:
# @Time: 12月 29, 2021
# ---
import datetime
import pandas as pd
import Trace.dfToClk as dk

pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.max_rows', None)  # 显示所有行


def insUmpToClk(database, tableNm, startTime):
    dbTable = database + '.' + tableNm
    querySql = f"""SELECT id, results, project, process, createtime
    FROM {database}.TraceMQResult prewhere id in
    (select id as id from {database}.TraceParaFailed prewhere createtime>toDateTime('{startTime}'));"""  # 查询一小时内的D49数据
    querySql2 = f"""select id from {dbTable} prewhere createtime>toStartOfHour(addHours(now(), -6))"""  # 查询分表已导入数据
    # try:
    res = dk.fromClick(querySql)
    df = pd.DataFrame(res)  # 转为df
    if df is None or len(df) == 0:
        print("无可更新数据~")
        return
    res2 = dk.fromClick(querySql2)
    df2 = pd.DataFrame(res2)  # 转为df
    df2 = df2.drop_duplicates(subset=[0])  # 去重：删除所有重复值
    # print(f"从{tableNm}数据库获取数据成功~")
    df = df.append(df2)
    df = df.append(df2)
    df = df.drop_duplicates(subset=[0], keep=False)  # 去重：删除所有重复值
    if df is None or len(df) == 0:
        print("无可更新数据~")
        return
    print("从数据库获取数据成功~")
    # except Exception as e:
    #     print("从数据库查询数据失败，报错代码为：" + str(e))
    #     return
    df2 = pd.DataFrame()
    # print(df2)
    for index, row in df.iterrows():  # 遍历df
        rowPd = pd.DataFrame(eval(row[1]))  # 字符串转list，展开json
        rowPd['id'] = row[0]
        rowPd['project'] = row[2]
        rowPd['process'] = row[3]
        rowPd['createtime'] = row[4]
        df2 = pd.concat([df2, rowPd], axis=0)
    df2 = df2.reset_index(drop=True)  # 重置索引
    # print(df2.columns)
    dk.toClick(df2, dbTable)  # df插入数据库
    print(f"写入数据库成功,已写入{len(df2)}条数据~")


# if __name__ == '__main__':
def RunTraceResultFailed(project):
    database = 'JGPWM' + project
    minutes = -30
    table = 'TraceResultFailed'
    startTime = (datetime.datetime.now() + datetime.timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
    print("-" * 20)
    print(f"插入{startTime}时段数据：")
    insUmpToClk(database, table, startTime)
