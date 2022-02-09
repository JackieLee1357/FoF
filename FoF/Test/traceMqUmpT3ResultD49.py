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

import sys
import pandas as pd
import dfToClk as dk

pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.max_rows', None)  # 显示所有行


def insUmpToClk(pjt, pcs, db):
    querySql = f"""SELECT id, results, project, process
            FROM JGPWM.view_TraceMQParameter 
            WHERE process='{pcs}' and project='{pjt}' AND results<>'' ;;"""  # 查询uvpu D49数据
    try:
        ump1D49SqlData = dk.fromClick(querySql)
        ump1D49RawData = pd.DataFrame(ump1D49SqlData)  # 转为df
        print("从数据库获取数据成功~")
    except Exception as e:
        print("从数据库查询数据失败，报错代码为：" + str(e))
        sys.exit(1)
    umpPd = pd.DataFrame()
    print(umpPd)
    for index, row in ump1D49RawData.iterrows():  # 遍历df
        rowPd = pd.DataFrame(eval(row[1]))  # 字符串转list，展开json
        rowPd['id'] = row[0]
        rowPd['project'] = row[2]
        rowPd['process'] = row[3]
        umpPd = pd.concat([umpPd, rowPd], axis=0)
    umpPd = umpPd.reset_index(drop=True)  # 重置索引
    columns = ['test', 'units', 'value', 'result', 'sub_test', 'lower_limit',
               'upper_limit', 'sub_sub_test', 'id', 'project', 'process']
    print(umpPd)
    print(umpPd.columns)
    try:
        dk.toClick(umpPd, db)  # df插入数据库
        print("写入数据库成功~")
    except Exception as e:
        print("插入数据库失败，报错代码为：" + str(e))
        sys.exit(1)


if __name__ == '__main__':
    project = "D49"
    process = "ump-t3"
    tableName = "TraceMQResultUmpT3"
    print(f"插入{project}专案数据：")
    insUmpToClk(project, process, tableName)
