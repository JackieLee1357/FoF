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

import pandas as pd
import dfToClk as dk

pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.max_rows', None)  # 显示所有行


def insUmpToClk(pjt, pcs, db, hour):
    querySql = f"""SELECT id, results, project, process
            FROM JGPWM.view_TraceMQParameter 
            WHERE process='{pcs}' and project='{pjt}' AND results<>''
            AND endtime>toStartOfHour(addHours(now(), {hour}));"""  # 查询一小时内的D49数据
    querySql2 = f"""select distinct id from JGPWMD49.{db}"""  # 查询分表已导入数据
    try:
        res = dk.fromClick(querySql)
        df = pd.DataFrame(res)  # 转为df
        if df is None or len(df) == 0:
            print("无可更新数据~")
            return
        res2 = dk.fromClick(querySql2)
        df2 = pd.DataFrame(res2)  # 转为df
        # print(f"从{db}数据库获取数据成功~")
        df = df.append(df2)
        df = df.append(df2)
        df = df.drop_duplicates(subset=[0], keep=False)  # 去重：删除所有重复值
        # print("从数据库获取数据成功~")
    except Exception as e:
        print("从数据库查询数据失败，报错代码为：" + str(e))
        return
    df2 = pd.DataFrame()
    # print(df2)
    for index, row in df.iterrows():  # 遍历df
        rowPd = pd.DataFrame(eval(row[1]))  # 字符串转list，展开json
        rowPd['id'] = row[0]
        rowPd['project'] = row[2]
        rowPd['process'] = row[3]
        df2 = pd.concat([df2, rowPd], axis=0)
    df2 = df2.reset_index(drop=True)  # 重置索引
    # print(df2)
    # print(df2.columns)
    try:
        dk.toClick(df2, db)  # df插入数据库
        print(f"{pcs}写入数据库成功,已写入{len(df2)}条数据~")
    except Exception as e:
        print("插入数据库失败，报错代码为：" + str(e))
        return


if __name__ == '__main__':
    project = "D49"
    hour = -1
    table = 'TraceResultAll'
    processToDb = ['alt-bg', 'alt-rcam', 'arc-nut-wld', 'bd-bc-qc', 'bg-assy', 'bt-wld', 'e75-nut-wld', 'e75-wld',
                   'fg-bc-qc', 'isra', 'mlb-nut-wld', 'mlb-nut-wld2', 'oqc-out', 'ort-final', 'ort-raw', 'rcam-assy',
                   'sp-wld', 'spk-nut-wld', 'ump-t1', 'ump-t3', 'volumax']
    for process in processToDb:
        print("-" * 20)
        print(f"插入{project}专案{process}制程数据：")
        try:
            insUmpToClk(project, process, table, hour)
        except Exception as e:
            print(f"插入数据库失败：{str(e)}")
            continue
