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


def openJson(df, col):  # 递归展开df中的json数据
    try:
        data = df[col].apply(lambda x: eval(x)).apply(pd.Series)  # 展开serials列
        if list(data.columns)[0] == 0:
            data.columns = col
        df = df.join(data).drop(columns=col)
    except:
        try:
            data = df[col].apply(pd.Series)  # 展开serials列
            if list(data.columns)[0] == 0:
                data.columns = col
            df = df.join(data).drop(columns=col)
        except Exception as e:
            # print("解析json失败，报错代码为：" + str(e))
            return df
    for i in data.columns:
        df = openJson(df, i)
    return df


def createTable(pjt, pcs, db):
    querySql = f"""SELECT *
            FROM JGPWM.view_TraceMQParameter WHERE process='{pcs}' and project='{pjt}';"""  # 查询总表数据
    try:
        res = dk.fromClick(querySql)
        df = pd.DataFrame(res)  # 转为df
        columns = ['id', 'project', 'process', 'sn', 'sntype', 'agent', 'testresult', 'lineid', 'starttime', 'endtime',
                   'fixtureid',
                   'logcreated', 'hiscreated', 'logevent', 'hisevent', 'stationid', 'serials', 'defects', 'data',
                   'results']
        df.columns = columns
        df = df.drop(columns='results')
    except Exception as e:
        print("从数据库查询数据失败，报错代码为：" + str(e))
        return
    df = openJson(df, 'serials')  # 展开该字段的json
    df = openJson(df, "data")
    try:
        # 如果数据库表不存在，根据columns自动创建数据库表，并导入数据。
        a = df.columns
        b = ' String,'.join(a)
        createSql = f"""create table {db}({b} String) engine = MergeTree ORDER BY id;"""
        createSql = createSql.replace("starttime String", "starttime DateTime")
        createSql = createSql.replace("endtime String", "endtime DateTime")
        createSql = createSql.replace("logcreated String", "logcreated DateTime")
        createSql = createSql.replace("hiscreated String", "hiscreated DateTime")
        createSql = createSql.replace("uut_stop String", "uut_stop DateTime")
        createSql = createSql.replace("uut_start String", "uut_start DateTime")
        print(createSql)
        dk.fromClick(createSql)
        print(f"{pcs}数据库表创建成功~")
    except Exception as e:
        print("创建数据库失败，报错代码为：" + str(e))
        pass
    return


def dropTable(process, table):
    print(f"删除{process}制程数据库表：")
    dropSql = f"""drop table {table}"""
    try:
        dk.fromClick(dropSql)
        print(f"删除{process}制程数据库表成功~")
    except:
        return


def queryTableName(processToDb):
    tableNames = {}
    for process, table in processToDb.items():
        print("-" * 20)
        print(f"创建{process}制据库表：")
        querySql = f"""select distinct name
        from system.columns
        where database = 'JGPWMD49'
        and table = '{table}';"""
        try:
            tableName = dk.fromClick(querySql)
            print(f"查询{process}制程数据库表成功~")
        except:
            return None
        colName = [i[0] for i in tableName]
        tableNames[table] = colName
    return tableNames


if __name__ == '__main__':
    project = "D49"
    processToDb = {'a-shear': 'TraceMQParaAShear', 'alt-bg': 'TraceMQParaAltBg', 'alt-mch': 'TraceMQParaAltMch',
                   'alt-rcam': 'TraceMQParaAltRcam', 'alt-sf': 'TraceMQParaAltSf', 'alt-spk': 'TraceMQParaAltSpk',
                   'ano': 'TraceMQParaAno', 'ano-qc': 'TraceMQParaAnoQc', 'arc-nut-wld': 'TraceMQParaArcNutWld',
                   'assy1-qc': 'TraceMQParaAssy1Qc', 'assy2-qc': 'TraceMQParaAssy2Qc', 'axi': 'TraceMQParaAxi',
                   'baking': 'TraceMQParaBaking', 'bd-bc-le': 'TraceMQParaBdBcLe', 'bd-bc-qc': 'TraceMQParaBdBcQc',
                   'bd-dp': 'TraceMQParaBdDp', 'bd-nut-wld': 'TraceMQParaBdNutWld', 'bg-assy': 'TraceMQParaBgAssy',
                   'bg-dp': 'TraceMQParaBgDp', 'bg-iqc': 'TraceMQParaBgIqc', 'br-assy': 'TraceMQParaBrAssy',
                   'br-bd-wld-r': 'TraceMQParaBrBdWldR', 'br-bd-wld-t': 'TraceMQParaBrBdWldT',
                   'bt-wld': 'TraceMQParaBtWld', 'bv-washer-assy': 'TraceMQParaBvWasherAssy',
                   'cg-spring-wld': 'TraceMQParaCgSpringWld', 'cleaning9': 'TraceMQParaCleaning9',
                   'cnc-qc': 'TraceMQParaCncQc', 'deano-cg': 'TraceMQParaDeanoCg', 'depu': 'TraceMQParaDepu',
                   'depvd-ad': 'TraceMQParaDepvdAd', 'depvd-bg': 'TraceMQParaDepvdBg', 'depvd-cg': 'TraceMQParaDepvdCg',
                   'depvd-sf': 'TraceMQParaDepvdSf', 'doe-in': 'TraceMQParaDoeIn',
                   'e75-nut-wld': 'TraceMQParaE75NutWld', 'e75-wld': 'TraceMQParaE75Wld', 'ex-lk': 'TraceMQParaExLk',
                   'extr-lk': 'TraceMQParaExtrLk', 'fg-bc-le': 'TraceMQParaFgBcLe', 'fg-bc-qc': 'TraceMQParaFgBcQc',
                   'grounding': 'TraceMQParaGrounding', 'ir-tt': 'TraceMQParaIrTt', 'isra': 'TraceMQParaIsra',
                   'mic2-assy': 'TraceMQParaMic2Assy', 'mic2-glu': 'TraceMQParaMic2Glu',
                   'mlb-nut-wld': 'TraceMQParaMlbNutWld', 'mlb-nut-wld2': 'TraceMQParaMlbNutWld2',
                   'oqc-in': 'TraceMQParaOqcIn', 'oqc-out': 'TraceMQParaOqcOut', 'ort-final': 'TraceMQParaOrtFinal',
                   'ort-raw': 'TraceMQParaOrtRaw', 'pl-ap': 'TraceMQParaPlAp', 'pm-ap': 'TraceMQParaPmAp',
                   'polish2-qc': 'TraceMQParaPolish2Qc', 'post-polish1-qc': 'TraceMQParaPostPolish1Qc',
                   'primer': 'TraceMQParaPrimer', 'rcam-assy': 'TraceMQParaRcamAssy',
                   'rcam-clean': 'TraceMQParaRcamClean', 'rew-in': 'TraceMQParaRewIn', 'sanding': 'TraceMQParaSanding',
                   'sb-qc': 'TraceMQParaSbQc', 'sf-assy': 'TraceMQParaSfAssy', 'sf-dp': 'TraceMQParaSfDp',
                   'snap-assy': 'TraceMQParaSnapAssy', 'sp-nut-wld': 'TraceMQParaSpNutWld',
                   'sp-shim-assy': 'TraceMQParaSpShimAssy', 'sp-wld': 'TraceMQParaSpWld',
                   'spk-assy': 'TraceMQParaSpkAssy', 'spk-bt-assy': 'TraceMQParaSpkBtAssy',
                   'spk-mesh': 'TraceMQParaSpkMesh', 'spk-nut-wld': 'TraceMQParaSpkNutWld',
                   'trch-bt-wld': 'TraceMQParaTrchBtWld', 'trim-assy': 'TraceMQParaTrimAssy',
                   'trim-br-wld': 'TraceMQParaTrimBrWld', 'ump-t1': 'TraceMQParaUmpT1', 'ump-t2': 'TraceMQParaUmpT2',
                   'ump-t3': 'TraceMQParaUmpT3', 'uvpu': 'TraceMQParaUvpu', 'vi': 'TraceMQParaVi',
                   'washer-assy': 'TraceMQParaWasherAssy', '': 'TraceMQPara'}
    for process, table in processToDb.items():
        print("-" * 20)
        print(f"创建{process}制据库表：")
        createTable(project, process, table)  # 创建数据库表
        # dropTable(process, table) # 删除数据库表

    # tableNames = queryTableName(processToDb)