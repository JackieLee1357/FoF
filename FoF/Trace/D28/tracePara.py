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
pd.set_option('display.max_rows', None)  # 显示所有行


def insUmpToClk(db, table, startTime):
    dbTable = db + '.' + table
    querySql = f"""SELECT id,
           project,
           process,
           sn,
           sntype,
           agent,
           testresult,
           lineid,
           a.createtime,
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
            FROM {db}.TraceMQLog a
            INNER JOIN {db}.TraceMQHistory b ON a.id = b.id
            prewhere a.createtime>toDateTime('{startTime}');"""
    querySql2 = f"""select id from {dbTable} prewhere createtime>toStartOfHour(addHours(now(), -3));"""  # 查询分表已导入数据
    # try:
    res = dk.fromClick(querySql)
    df = pd.DataFrame(res)  # 转为df
    if df is None or len(df) == 0:
        print("无可更新数据~")
        return
    res2 = dk.fromClick(querySql2)
    df2 = pd.DataFrame(res2)  # 转为df
    df = df.append(df2)
    df = df.append(df2)
    df = df.drop_duplicates(subset=[0], keep=False)  # 去重：删除所有重复值
    print("从view数据库获取数据成功~")
    columns = ['id', 'project', 'process', 'sn', 'sntype', 'agent', 'testresult', 'lineid', 'createtime',
               'starttime', 'endtime',
               'fixtureid', 'logcreated', 'hiscreated', 'logevent', 'hisevent', 'stationid', 'serials', 'defects',
               'data']
    df.columns = columns
    # except Exception as e:
    #     print("从数据库查询数据失败，报错代码为：" + str(e))
    #     return
    df = openJson(df, 'serials')
    df = openJson(df, "data")
    df.fillna('', inplace=True)  # 替换Nan值
    # try:
    dk.toClick(df, dbTable)  # df插入数据库
    print(f"写入数据库成功,已写入{len(df)}条数据~")
    # except Exception as e:
    #     print("插入数据库失败，报错代码为：" + str(e))
    #     return


def openJson(df, col):  # 递归展开df中的json数据
    try:
        data0 = df[col].apply(lambda x: eval(x) if (x != '') else x)
        data = data0.apply(pd.Series)  # 展开serials列
        if len(list(data.columns)) == 1:
            data.columns = col
        else:
            try:
                data = data.drop(columns=0)
            except:
                pass
        data.fillna('', inplace=True)  # 替换Nan值
        df = df.join(data).drop(columns=col)
    except:
        try:
            data = df[col].apply(pd.Series)  # 展开serials列
            if len(list(data.columns)) == 1:
                data.columns = col
            else:
                try:
                    data = data.drop(columns=0)
                except:
                    pass
            data.fillna('', inplace=True)  # 替换Nan值
            df = df.join(data).drop(columns=col)
        except Exception as e:
            # print("解析json失败，报错代码为：" + str(e))
            return df
    if len(data.columns) > 15:
        return df
    for i in data.columns:
        for j in df[i]:
            if isinstance(j, dict):
                df = openJson(df, i)
                break
    return df


# if __name__ == '__main__':
def RunTracePara(project):
    minutes = -25
    table = 'TraceParaAll'
    database = 'JGPWM' + project
    startTime = (datetime.datetime.now() + datetime.timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
    print("-" * 20)
    print(f"插入{startTime}时段数据：")
    insUmpToClk(database, table, startTime)
    # try:
    #     ins_to_ch(database, table, startTime)
    # except Exception as e:
    #     print(f"插入数据库失败：{str(e)}")
