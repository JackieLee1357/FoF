#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: traceAutoDel.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 1月 15, 2022
# ---
import pandas as pd
import Trace.dfToClk as dk


def delFromClk(database):
    querySql = f"""select id from {database}.TraceParaAll prewhere createtime>addMinutes(now(), -1440);"""
    querySql2 = f"""select id from {database}.TraceParaAll prewhere createtime>addMinutes(now(), -1440);"""
    res = dk.fromClick(querySql)
    if res is None or len(res) == 0:
        print("无数据可删除~")
        return
    df = pd.DataFrame(res)  # 转为df
    df = df.drop_duplicates(subset=[0])  # 去重：删除所有重复值
    print(f"从获取{len(df)}条数据成功~")
    ids = "'" + "','".join(df[0]) + "'"
    delSql = f"""alter table {database}.TraceMQHistory delete where id in({ids});"""
    delSql2 = f"""alter table {database}.TraceMQLog delete where id in({ids});"""  # 查询分表已导入数据
    dk.fromClick(delSql)
    dk.fromClick(delSql2)
    print(f"从{database}.TraceMQHistory和{database}.TraceMQLog删除{len(df)}条数据成功~")


# if __name__ == '__main__':
def RunAutoDel(project):
    # project = 'D49'
    database = 'JGPWM' + project
    print("-" * 20)
    print(f"删除数据：")
    delFromClk(database)


# RunAutoDel('D16')
#

