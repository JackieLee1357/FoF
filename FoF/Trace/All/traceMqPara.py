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

import D49.dfToClk as dk

pd.set_option('display.max_columns', None)  # 显示所有列


# pd.set_option('display.max_rows', None)  # 显示所有行


def insUmpToClk(db, hour):
    queryLog = f"""SELECT TOP (10000) id,
           project,
           process,
           agent,
           created AS logcreated,
           event   AS logevent,
           serials,
           defects,
           data
            FROM JGPWM.TraceMQLog;"""
    colLog = ['id', 'project', 'process', 'agent', 'logcreated', 'logevent', 'serials', 'defects', 'data']
    queryHis = f"""SELECT TOP (10000) id,
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
           created AS hiscreated,
           event   AS hisevent,
           stationid
            FROM JGPWM.TraceMQHistory;"""
    colHis = ['id', 'project', 'process', 'sn', 'sntype',
              'agent', 'testresult', 'lineid', 'starttime', 'endtime',
              'fixtureid', 'hiscreated', 'hisevent', 'stationid']
    queryPara = f"""select id from JGPWM.TracePara 
    WHERE hiscreated>toStartOfHour(addHours(now(), {hour}));"""  # 查询分表已导入数据
    try:
        resLog = dk.fromClick(queryLog)
        dfLog = pd.DataFrame(resLog)  # 转为df
        dfLog.columns = colLog
        resHis = dk.fromClick(queryHis)
        dfHis = pd.DataFrame(resHis)  # 转为df
        dfHis.columns = colHis
        ids = pd.merge(dfLog, dfHis, on=['id', 'project', 'process', 'agent'], how="inner")
        resPara = dk.fromClick(queryPara)
        dfPara = pd.DataFrame(resPara)  # 转为df
        try:
            dfPara.columns = ['id']
            df = pd.merge(ids, dfPara, on=["id"], how="inner")  # 需要插入数据库的id信息
        except:
            df = ids
        # print(df)
        if (df is None) or (len(df) == 0):
            print("无可更新数据~")
            return
    except Exception as e:
        print("从数据库查询数据失败，报错代码为：" + str(e))
        return
    try:
        # print(df.columns)

        # df = open_json(df, 'serials')

        # print(df)
        # print("222222222222222222222222")
        df = openJson(df, "data")
        # print(df)
        print(df.columns)
        print("33333333333333333333333333")
        dk.toClick(df, db)  # df插入数据库
        print(f"写入数据库成功,已写入{len(df)}条数据~")
        idList = df['id']
        try:
            for id in list(idList):
                delLog = f"""alter table JGPWM.TraceMQLog delete where id='{id}';"""
                delRes = f"""alter table JGPWM.TraceMQHis delete where id='{id}';"""
                dk.fromClick(delRes)
                dk.fromClick(delLog)
                print("删除数据成功~")
        except Exception as e:
            print(f"删除数据失败:{str(e)}")
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
    hour = -10000
    table = 'JGPWM.TracePara'
    print("-" * 20)
    print(f"插入制程数据：")
    try:
        insUmpToClk(table, hour)
    except Exception as e:
        print(f"插入数据库失败：{str(e)}")
