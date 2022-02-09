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


def insUmpToClk(pjt, pcs, db, hour):
    # querySql = f"""SELECT *
    #         FROM JGPWM.view_TraceMQParameter WHERE process='{pcs}' and project='{pjt}'
    #         AND endtime>toStartOfHour(addHours(now(), {hour}));"""  # 查询一小时内的D49数据
    querySql = f"""SELECT id,
           project,
           process,
           sn,
           sntype,
           agent,
           testresult,
           lineid,
           starttime,
           endtime,
           fixtureid,
           a.created AS logcreated,
           b.created AS hiscreated,
           a.event   AS logevent,
           b.event   AS hisevent,
           stationid,
           serials,
           defects,
           data
            FROM JGPWMD49.TraceMQLog AS a
            INNER JOIN JGPWMD49.TraceMQHistory AS b ON a.id = b.id
            WHERE endtime>toStartOfHour(addHours(now(), {hour}))
            and process='{pcs}' and project='{pjt}';"""
    querySql2 = f"""select distinct id from JGPWMD49.{db};"""  # 查询分表已导入数据
    # print(querySql)
    # print(querySql2)
    try:
        res = dk.fromClick(querySql)
        df = pd.DataFrame(res)  # 转为df
        if df is None or len(df) == 0:
            print("无可更新数据~")
            return
        # print(df)
        res2 = dk.fromClick(querySql2)
        df2 = pd.DataFrame(res2)  # 转为df
        # print(df2)
        # print(f"从{db}数据库获取数据成功~")
        df = df.append(df2)
        df = df.append(df2)
        df = df.drop_duplicates(subset=[0], keep=False)  # 去重：删除所有重复值
        # print("从view数据库获取数据成功~")
        columns = ['id', 'project', 'process', 'sn', 'sntype', 'agent', 'testresult', 'lineid', 'starttime', 'endtime',
                   'fixtureid', 'logcreated', 'hiscreated', 'logevent', 'hisevent', 'stationid', 'serials', 'defects',
                   'data']
        df.columns = columns
    except Exception as e:
        print("从数据库查询数据失败，报错代码为：" + str(e))
        return
    df = openJson(df, 'serials')
    df = openJson(df, "data")
    # print(df)
    print(df.columns)
    try:
        dk.toClick(df, db)  # df插入数据库
        print(f"{pcs}写入数据库成功,已写入{len(df)}条数据~")
    except Exception as e:
        print("插入数据库失败，报错代码为：" + str(e))
        return


def openJson(df, col):  # 递归展开df中的json数据
    print(col)
    try:
        data = df[col].apply(lambda x: eval(x) if (x != '') else x).apply(pd.Series)  # 展开serials列
        if len(list(data.columns)) == 0:
            data.columns = col
        else:
            data = data.drop(columns=0)
        data.fillna('', inplace=True)
        df = df.join(data).drop(columns=col)
    except:
        try:
            data = df[col].apply(pd.Series)  # 展开serials列
            if len(list(data.columns)) == 0:
                data.columns = col
            else:
                data = data.drop(columns=0)
            df = df.join(data).drop(columns=col)
        except Exception as e:
            # print("解析json失败，报错代码为：" + str(e))
            return df
    for i in data.columns:
        df = openJson(df, i)
    return df


if __name__ == '__main__':
    project = "D49"
    hour = -1
    table = 'TraceParaAll'
    processToDb = ['a-shear', 'alt-bg', 'alt-rcam', 'ano', 'ano-qc', 'arc-nut-wld', 'assy1-qc', 'assy2-qc', 'bd-bc-le',
                   'bd-bc-qc', 'bg-assy', 'bg-eta', 'bt-wld', 'doe-in', 'e75-nut-wld', 'e75-wld', 'extr-lk', 'fg-bc-le',
                   'fg-bc-qc', 'fgi', 'isra', 'mlb-nut-wld', 'mlb-nut-wld2', 'oqc', 'oqc-in', 'oqc-out', 'ort-final',
                   'ort-raw', 'rcam-assy', 'rew-in', 'sb-qc', 'ship', 'si-out', 'sp-wld', 'spk-nut-wld', 'ump-t1',
                   'ump-t3', 'volumax', 'wh-in']

    for process in processToDb:
        print("-" * 20)
        print(f"插入{project}专案{process}制程数据：")
        try:
            insUmpToClk(project, process, table, hour)
        except Exception as e:
            print(f"插入数据库失败：{str(e)}")
            continue



