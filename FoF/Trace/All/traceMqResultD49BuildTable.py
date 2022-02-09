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


def createTable(pjt, pcs, db):
    querySql = f"""SELECT id, results, project, process
            FROM JGPWM.view_TraceMQParameter 
            WHERE process='{pcs}' and project='{pjt}' AND results<>'' ;;"""  # 查询uvpu D49数据
    try:
        res = dk.fromClick(querySql)
        df = pd.DataFrame(res)  # 转为df
    except Exception as e:
        print("从数据库查询数据失败，报错代码为：" + str(e))
        return
    df2 = pd.DataFrame()
    for index, row in df.iterrows():  # 遍历df
        rowPd = pd.DataFrame(eval(row[1]))  # 字符串转list，展开json
        rowPd['id'] = row[0]
        rowPd['project'] = row[2]
        rowPd['process'] = row[3]
        df2 = pd.concat([df2, rowPd], axis=0)
    df2 = df2.reset_index(drop=True)  # 重置索引
    # print(df2)
    try:
        # 如果数据库表不存在，根据columns自动创建数据库表，并导入数据。
        a = df2.columns
        b = ' String,'.join(a)
        createSql = f"""create table {db}({b} String) engine = MergeTree ORDER BY id;"""
        print(createSql)
        dk.fromClick(createSql)
        print(f"{pcs}数据库创建成功成功~")
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
    processToDb = {'a-shear': 'TraceMQResultAShear', 'alt-bg': 'TraceMQResultAltBg', 'alt-mch': 'TraceMQResultAltMch',
                   'alt-rcam': 'TraceMQResultAltRcam', 'alt-sf': 'TraceMQResultAltSf', 'alt-spk': 'TraceMQResultAltSpk',
                   'ano': 'TraceMQResultAno', 'ano-qc': 'TraceMQResultAnoQc', 'arc-nut-wld': 'TraceMQResultArcNutWld',
                   'assy1-qc': 'TraceMQResultAssy1Qc', 'assy2-qc': 'TraceMQResultAssy2Qc', 'axi': 'TraceMQResultAxi',
                   'baking': 'TraceMQResultBaking', 'bd-bc-le': 'TraceMQResultBdBcLe',
                   'bd-bc-qc': 'TraceMQResultBdBcQc', 'bd-dp': 'TraceMQResultBdDp',
                   'bd-nut-wld': 'TraceMQResultBdNutWld', 'bg-assy': 'TraceMQResultBgAssy',
                   'bg-dp': 'TraceMQResultBgDp', 'bg-iqc': 'TraceMQResultBgIqc', 'br-assy': 'TraceMQResultBrAssy',
                   'br-bd-wld-r': 'TraceMQResultBrBdWldR', 'br-bd-wld-t': 'TraceMQResultBrBdWldT',
                   'bt-wld': 'TraceMQResultBtWld', 'bv-washer-assy': 'TraceMQResultBvWasherAssy',
                   'cg-spring-wld': 'TraceMQResultCgSpringWld', 'cleaning9': 'TraceMQResultCleaning9',
                   'cnc-qc': 'TraceMQResultCncQc', 'deano-cg': 'TraceMQResultDeanoCg', 'depu': 'TraceMQResultDepu',
                   'depvd-ad': 'TraceMQResultDepvdAd', 'depvd-bg': 'TraceMQResultDepvdBg',
                   'depvd-cg': 'TraceMQResultDepvdCg', 'depvd-sf': 'TraceMQResultDepvdSf',
                   'doe-in': 'TraceMQResultDoeIn', 'e75-nut-wld': 'TraceMQResultE75NutWld',
                   'e75-wld': 'TraceMQResultE75Wld', 'ex-lk': 'TraceMQResultExLk', 'extr-lk': 'TraceMQResultExtrLk',
                   'fg-bc-le': 'TraceMQResultFgBcLe', 'fg-bc-qc': 'TraceMQResultFgBcQc',
                   'grounding': 'TraceMQResultGrounding', 'ir-tt': 'TraceMQResultIrTt', 'isra': 'TraceMQResultIsra',
                   'mic2-assy': 'TraceMQResultMic2Assy', 'mic2-glu': 'TraceMQResultMic2Glu',
                   'mlb-nut-wld': 'TraceMQResultMlbNutWld', 'mlb-nut-wld2': 'TraceMQResultMlbNutWld2',
                   'oqc-in': 'TraceMQResultOqcIn', 'oqc-out': 'TraceMQResultOqcOut',
                   'ort-final': 'TraceMQResultOrtFinal', 'ort-raw': 'TraceMQResultOrtRaw', 'pl-ap': 'TraceMQResultPlAp',
                   'pm-ap': 'TraceMQResultPmAp', 'polish2-qc': 'TraceMQResultPolish2Qc',
                   'post-polish1-qc': 'TraceMQResultPostPolish1Qc', 'primer': 'TraceMQResultPrimer',
                   'rcam-assy': 'TraceMQResultRcamAssy', 'rcam-clean': 'TraceMQResultRcamClean',
                   'rew-in': 'TraceMQResultRewIn', 'sanding': 'TraceMQResultSanding', 'sb-qc': 'TraceMQResultSbQc',
                   'sf-assy': 'TraceMQResultSfAssy', 'sf-dp': 'TraceMQResultSfDp', 'snap-assy': 'TraceMQResultSnapAssy',
                   'sp-nut-wld': 'TraceMQResultSpNutWld', 'sp-shim-assy': 'TraceMQResultSpShimAssy',
                   'sp-wld': 'TraceMQResultSpWld', 'spk-assy': 'TraceMQResultSpkAssy',
                   'spk-bt-assy': 'TraceMQResultSpkBtAssy', 'spk-mesh': 'TraceMQResultSpkMesh',
                   'spk-nut-wld': 'TraceMQResultSpkNutWld', 'trch-bt-wld': 'TraceMQResultTrchBtWld',
                   'trim-assy': 'TraceMQResultTrimAssy', 'trim-br-wld': 'TraceMQResultTrimBrWld',
                   'ump-t1': 'TraceMQResultUmpT1', 'ump-t2': 'TraceMQResultUmpT2', 'ump-t3': 'TraceMQResultUmpT3',
                   'uvpu': 'TraceMQResultUvpu', 'vi': 'TraceMQResultVi', 'washer-assy': 'TraceMQResultWasherAssy',
                   '': 'TraceMQResult'}

    for process, table in processToDb.items():
        print("-" * 20)
        print(f"创建{process}制据库表：")
        createTable(project, process, table)  # 创建数据库表
        # dropTable(process, table) # 删除数据库表

    # tableNames = queryTableName(processToDb)
    # print(tableNames)
