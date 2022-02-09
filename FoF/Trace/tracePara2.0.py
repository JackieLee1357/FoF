#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: tracePara.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site:
# @Time: 1月 19, 2022
# ---
import datetime
import logging
import pandas as pd
import Trace.dfToClk as dk


def insUmpToClk(project, table):
    db = 'JGPWM' + project
    dbTable = db + '.' + table
    querySql1 = f"""select id from JGPWM.TraceLog prewhere project='{project}';"""  # 查询分表已导入数据
    querySql2 = f"""select id from JGPWM.TraceHistory prewhere project='{project}';"""  # 查询分表已导入数据
    res1 = dk.fromClick(querySql1)
    df1 = pd.DataFrame(res1)  # 转为df
    if df1 is None or len(df1) == 0:
        print("1无可更新数据~")
        return
    res2 = dk.fromClick(querySql2)
    df2 = pd.DataFrame(res2)  # 转为df
    if df2 is None or len(df2) == 0:
        print("2无可更新数据~")
        return
    df = pd.merge(df1, df2, how='inner', on=0)
    if df is None or len(df) == 0:
        print("3无可更新数据~")
        return
    df = df.drop_duplicates(subset=[0])  # 去重：删除所有重复值
    if len(df) > 100000:
        df = df.loc[0:100000, 0:]
    ids = "'" + "','".join(df[0]) + "'"
    querySql3 = f"""SELECT id, process, event AS logevent, band, bg, sp, fg,
        defects, items, uut_attributes, test_attributes, test_station_attributes FROM JGPWM.TraceLog prewhere project='{project}' and id in ({ids});"""
    querySql4 = f"""SELECT id, sn, sntype, agent, testresult, lineid, starttime, endtime, fixtureid,
         event AS hisevent, stationid FROM JGPWM.TraceHistory 
        prewhere project='{project}' and id in ({ids});"""
    columns3 = ['id', 'process', 'logevent', 'band', 'bg', 'sp', 'fg', 'defects',
                'items', 'uut_attributes', 'test_attributes', 'test_station_attributes']
    columns4 = ['id', 'sn', 'sntype', 'agent', 'testresult', 'lineid', 'starttime', 'endtime',
                'fixtureid', 'hisevent', 'stationid']
    res3 = dk.fromClick(querySql3)
    df3 = pd.DataFrame(res3)  # 转为df
    if df3 is None or len(df3) == 0:
        print("4无可更新数据~")
        return
    df3.columns = columns3
    res4 = dk.fromClick(querySql4)
    df4 = pd.DataFrame(res4)  # 转为df
    if df4 is None or len(df4) == 0:
        print("5无可更新数据~")
        return
    df4.columns = columns4
    df = pd.merge(df3, df4, how='inner', on='id')
    if df is None or len(df) == 0:
        print("6无可更新数据~")
        return
    df = df.drop_duplicates(subset=['id'])  # 去重：删除所有重复值
    print(F"从view数据库获取{len(df)}条数据成功~")
    df = openJson(df, 'items')
    df = openJson(df, 'uut_attributes')
    df = openJson(df, 'test_attributes')
    df = openJson(df, 'test_station_attributes')
    df.fillna('', inplace=True)  # 替换Nan值
    df['createtime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 数据插入数据库时间
    print(f"获取{len(df)}条数据,准备插入数据库~")
    df = df.sort_values(['process', 'createtime'])
    dk.toClick(df, dbTable)  # df插入数据库
    print(f"写入数据库成功,已写入{len(df)}条数据~")
    ids = "'" + "','".join(df['id']) + "'"  # 删除数据
    delSql = f"""alter table JGPWM.TraceLog delete where project='{project}' and id in({ids});"""
    delSql2 = f"""alter table JGPWM.TraceHistory delete where project='{project}' and id in({ids});"""  # 查询分表已导入数据
    dk.fromClick(delSql)
    dk.fromClick(delSql2)
    print(f"从JGPWM.TraceLog和JGPWM.TraceHistory删除{len(df)}条数据成功~")


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
    # if len(data.columns) > 10:
    #     return df
    # for i in data.columns:
    #     for j in df[i]:
    #         if isinstance(j, dict):
    #             df = open_json(df, i)
    #             break
    return df


# //TraceBin定时插入任务
def runTraceBin(project, table):
    db = 'JGPWM' + project
    insertSql = f"""
    insert into {db}.view_TraceResultBin select  
            a.id
            ,b.sn
            ,b.lineid as line
            ,b.stationid as station
            ,b.RECIPE as color
            ,b.SIM_PROFILEID as bin
            ,b.logevent as event
            ,a.createtime
            ,visitParamExtractString(arrayJoin(JSONExtractArrayRaw(a.results)),'sub_test') as sub_test
            ,visitParamExtractFloat(arrayJoin(JSONExtractArrayRaw(a.results)),'value') as value
    from {db}.view_TraceResult1Month a inner join {db}.TracePara1Month b on a.id = b.id
    where a.process='ump-t1' and sub_test='413.L' and createtime>addHours(now(),-1)
    ORDER BY createtime DESC;
    """
    dk.fromClick(insertSql)
    logging.Logger("插入TraceBin数据完成~")


# if __name__ == '__main__':
def RunTracePara(project):
    table = 'TracePara1Day'
    print("-" * 20)
    print(f"插入数据：")
    insUmpToClk(project, table)
    runTraceBin(project, table)

# RunTracePara('D49')
