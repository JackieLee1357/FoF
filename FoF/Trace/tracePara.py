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
import json
import logging
import pandas as pd
import Trace.dfToClk as dk


def ins_to_ch(project, table):
    """
    读取数据处理后插入到clickhouse
    :param project:
    :param table:
    :return:
    """
    db = 'JGPWM' + project
    db_table = db + '.' + table
    query_sql1 = f"""select id from JGPWM.TraceLog prewhere project='{project}';"""  # 查询分表已导入数据
    query_sql2 = f"""select id from JGPWM.TraceHistory prewhere project='{project}';"""  # 查询分表已导入数据
    res1 = dk.fromClick(query_sql1)
    df1 = pd.DataFrame(res1)  # 转为df
    if df1 is None or len(df1) == 0:
        print("1无可更新数据~")
        return
    res2 = dk.fromClick(query_sql2)
    df2 = pd.DataFrame(res2)  # 转为df
    if df2 is None or len(df2) == 0:
        print("2无可更新数据~")
        return
    df = pd.merge(df1, df2, on=0)
    if df is None or len(df) == 0:
        print("3无可更新数据~")
        return
    df = df.drop_duplicates(subset=[0])  # 去重：删除所有重复值
    if len(df) > 100000:
        df = df.loc[0:100000, 0:]
    ids = "'" + "','".join(df[0]) + "'"
    query_sql3 = f"""SELECT id, process, event AS logevent, band, bg, sp, fg,
        defects, items, uut_attributes, test_attributes, test_station_attributes FROM JGPWM.TraceLog 
        prewhere project='{project}' and id in ({ids});"""
    query_sql4 = f"""SELECT id, sn, sntype, agent, testresult, lineid, starttime, endtime, fixtureid,
         event AS hisevent, stationid FROM JGPWM.TraceHistory 
        prewhere project='{project}' and id in ({ids});"""
    columns3 = ['id', 'process', 'logevent', 'band', 'bg', 'sp', 'fg', 'defects',
                'items', 'uut_attributes', 'test_attributes', 'test_station_attributes']
    columns4 = ['id', 'sn', 'sntype', 'agent', 'testresult', 'lineid', 'starttime', 'endtime',
                'fixtureid', 'hisevent', 'stationid']
    res3 = dk.fromClick(query_sql3)
    df3 = pd.DataFrame(res3)  # 转为df
    if df3 is None or len(df3) == 0:
        print("4无可更新数据~")
        return
    df3.columns = columns3
    res4 = dk.fromClick(query_sql4)
    df4 = pd.DataFrame(res4)  # 转为df
    if df4 is None or len(df4) == 0:
        print("5无可更新数据~")
        return
    df4.columns = columns4
    df = pd.merge(df3, df4, on='id')
    if df is None or len(df) == 0:
        print("6无可更新数据~")
        return
    df = df.drop_duplicates(subset=['id'])  # 去重：删除所有重复值
    print(F"从view数据库获取{len(df)}条数据成功~")
    df.fillna('', inplace=True)  # 替换Nan值
    df = open_json(df, 'test_station_attributes')
    df = open_json(df, 'uut_attributes')
    df = open_json(df, 'test_attributes')
    df = open_json(df, 'items')
    df.fillna('', inplace=True)  # 替换Nan值
    df['createtime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 数据插入数据库时间
    print(f"获取{len(df)}条数据,准备插入数据库~")
    df = df.sort_values(['process', 'createtime'])
    dk.toClick(df, db_table)  # df插入数据库
    print(f"写入数据库成功,已写入{len(df)}条数据~")
    ids = "'" + "','".join(df['id']) + "'"  # 删除数据
    del_sql = f"""alter table JGPWM.TraceLog delete where project='{project}' and id in({ids});"""
    del_sql2 = f"""alter table JGPWM.TraceHistory delete where project='{project}' and id in({ids});"""  # 查询分表已导入数据
    dk.fromClick(del_sql)
    dk.fromClick(del_sql2)
    print(f"从JGPWM.TraceLog和JGPWM.TraceHistory删除{len(df)}条数据成功~")


def open_json(df, col):  # 递归展开df中的json数据
    """
    展开Json
    :param df:
    :param col:
    :return:
    """
    try:
        print(f"解析{col}:")
        # null = None  # 解决报错：NameError: name 'null' is not defined
        # data0 = df[col].apply(lambda x: eval(x) if (x != '') else x)
        data0 = df[col].apply(lambda x: json.loads(x) if (x != '') else x)
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
        print(f"解析{col}成功~")
    except Exception as e:
        print(f"解析{col}失败，报错代码为：" + str(e))
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
            print("解析json失败，报错代码为：" + str(e))
            return df
    return df


def runTraceBin(project):
    """
    TraceBin定时插入任务
    :param project:
    """
    db = 'JGPWM' + project
    insert_sql = f"""
    insert into {db}.view_TraceSimBin select
            b.sn
            ,b.stationid as station
            ,b.RECIPE as color
            ,b.SIM_PROFILEID as bin
            ,b.logevent as event
            ,a.createtime
            ,visitParamExtractFloat(arrayJoin(JSONExtractArrayRaw(a.results)),'value') as value
    from {db}.view_TraceResult1Day a inner join {db}.TracePara1Day b on a.id = b.id
    where createtime>=addHours(now(),-1) and a.process='ump-t1'
    and visitParamExtractString(arrayJoin(JSONExtractArrayRaw(a.results)),'sub_test')='413.L'
    ORDER BY createtime DESC;
    """
    optimize_sql = f"""
        OPTIMIZE TABLE {db}.view_TraceSimBin FINAL;
    """
    dk.fromClick(insert_sql)
    dk.fromClick(optimize_sql)
    print("插入SimBin数据完成~")


# if __name__ == '__main__':
def RunTracePara(project):
    """
    运行数据处理任务
    :param project:
    """
    table = 'TracePara1Day'
    print("-" * 20)
    print(f"插入数据：")
    ins_to_ch(project, table)
    runTraceBin(project)

# RunTracePara('D49')
