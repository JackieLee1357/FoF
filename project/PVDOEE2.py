#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: PVDOEE2.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 11月 24, 2021
# ---


import configparser
import datetime
import os
import sys
import pandas
import pymssql
import sqlalchemy


# 从SQL Server获取数据
def getDataFromSqlS(sql):
    try:
        conn = pymssql.connect(
            host='CNWXIM0TRSQLV4A',  # 主机名或ip地址
            user='PBIuser',  # 用户名
            password='PBIUser123456',  # 密码
            charset='utf8',  # 字符编码
            database='PVD')  # 库名
        cursor = conn.cursor()
        cursor.execute(sql)
        df = pandas.DataFrame(cursor.fetchall())
        cursor.close()
        conn.close()
        return df
    except pymssql.Error as e:
        print(e)
        return None


# 去除未报警的记录
def dataDuplicated(frame, eventTime, endTime):
    frame.iloc[0, 3] = pandas.to_datetime(eventTime)  # 第一行时间改为开始时间
    frame['lastsTime'] = 0
    for i in range(len(frame)):
        if i == len(frame) - 1:
            frame.loc[i, 'lastsTime'] = (pandas.to_datetime(endTime) - frame.iloc[i, 3]).seconds  # 计算最后一行时间
        else:
            frame.loc[i, 'lastsTime'] = (frame.iloc[i + 1, 3] - frame.iloc[i, 3]).seconds
    # print('数据清洗完毕')
    return frame


# 计算downtime
def statusCalculate(fms, emt, eventDate):
    status1 = 0
    status2 = 0
    status3 = 0
    status4 = 0
    status5 = 0
    status6 = 0
    status7 = 0
    downtime = fms.groupby("status").agg({'lastsTime': 'sum'})
    downtime = downtime.reset_index()
    for i in range(len(downtime)):
        if downtime.iloc[i, 0] == 1:
            status1 = downtime.loc[i, 'lastsTime']
        elif downtime.iloc[i, 0] == 2:
            status2 = downtime.loc[i, 'lastsTime']
        elif downtime.iloc[i, 0] == 3:
            status3 = downtime.loc[i, 'lastsTime']
        elif downtime.iloc[i, 0] == 4:
            status4 = downtime.loc[i, 'lastsTime']
        elif downtime.iloc[i, 0] == 5:
            status5 = downtime.loc[i, 'lastsTime']
        elif downtime.iloc[i, 0] == 6:
            status6 = downtime.loc[i, 'lastsTime']
        elif downtime.iloc[i, 0] == 7:
            status7 = downtime.loc[i, 'lastsTime']

    dt = pandas.DataFrame([[emt, eventDate, status1, status2, status3, status4, status5, status6, status7]])
    dt.columns = ["emt", "eventTime", "status1", "status2", "status3", "status4", "status5", "status6",
                  "status7"]  # 修改列名
    # print("===========================")
    return dt


# 计算downtime
def errorCodeCalculate(fms, emt, eventDate):
    columns = ['emt', 'status', 'ErrorCode', 'eventTime', 'lastTime']
    fms.columns = columns
    fms = fms[fms['status'] == '3']
    fms = fms[fms['ErrorCode'] != '']
    fms.reset_index(drop=True, inplace=True)  # 行号重新排序
    errorCodeTime = fms.groupby('ErrorCode').agg({'lastTime': 'sum'})
    errorCodeTime = errorCodeTime.reset_index()
    errorCodeTime['emt'] = emt
    errorCodeTime['status'] = '3'
    errorCodeTime['eventTime'] = eventDate
    errorCodeTime.columns = ['ErrorCode', 'lastTime', 'emt', 'status', 'eventTime']
    errorCodeTime = errorCodeTime[['emt', 'status', 'ErrorCode', 'eventTime', 'lastTime']]
    # print(errorCodeTime)
    # print("===========================")
    return errorCodeTime


def insert_intosql(frame, tableName):
    # 数据插入PG sql
    db_url = 'mssql+pymssql://PBIuser:PBIUser123456@CNWXIM0TRSQLV4A/Metal_Robot'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    frame.to_sql(tableName, engine, index=False, if_exists='append', method='multi', index_label=None,
                 chunksize=None, )
    engine.dispose()
    # print('数据写入SQL Server成功')
    # print('------------')


def conPGSQL(tableName):
    connection = pymssql.connect(
        host='CNWXIM0TRSQLV4A',  # 主机名或ip地址
        user='PBIuser',  # 用户名
        password='PBIUser123456',  # 密码
        charset='UTF-8',  # 字符编码
        database='Metal_Robot')  # 库名
    cursor = connection.cursor()
    cursor.execute(f'select distinct eventtime from {tableName}')  # order by eventtime desc
    row = cursor.fetchall()
    row = [i[0].strftime("%Y-%m-%d %H") + ':00:00' for i in row]  # 时间转化为字符串
    # row = [''.join(a) for a in row]  # 元组转化为列表
    connection.commit()
    cursor.close()
    return row


def runPVDOEE():
    timer = datetime.datetime.now()
    pandas.set_option('display.max_columns', None)  # 显示所有列
    pandas.set_option('display.max_rows', None)  # 显示所有行
    path0 = os.path.abspath(os.path.dirname(sys.argv[0]))  # 获取当前执行文件夹路径
    path0 = path0 + "/PVDOEE.ini"
    config = configparser.ConfigParser()
    try:
        config.read(path0, encoding="utf-8-sig")  # 导出配置文件,解决文档格式为utf-8-bom问题
    except Exception as e:
        print(e)
        config.read(path0, encoding="utf-8")  # 导出配置文件
    beginHour = int(config.get("messages", "beginHour"))  # 查询开始时间：多少小时之前
    endHour = int(config.get("messages", "endHour"))  # 查询结束时间：多少小时之前
    tableName = 'PVD_output'
    pd = pandas.DataFrame()
    times = conPGSQL(tableName)
    for i in range(endHour - 1, beginHour - 1, -1):  # 按小时倒序循环
        maxTime = i + 1
        minTime = i
        j = -maxTime
        eventDate = str(
            (datetime.datetime.now() + datetime.timedelta(hours=minTime)).strftime(f"%Y-%m-%d %H:00:00"))  # 发生时段
        endTime = str(
            (datetime.datetime.now() + datetime.timedelta(hours=maxTime)).strftime(f"%Y-%m-%d %H:00:00"))  # 时段结束时间
        print('output开始时间' + eventDate)
        if eventDate in times:
            print('该时段数据已处理')
            continue
        columns = ['emt', 'output', 'eventtime', 'hourindex']
        sql = f"""WITH ONE AS(
            SELECT EMT
            ,DATEDIFF(MINUTE,'{eventDate}',EventTime)/60+1 hourIndex
            ,max(ChamberNumber) maxTotalNum
            ,min(ChamberNumber) minTotalNum
            ,COUNT(ChamberNumber) totalCount
--             	   ,avg( CONVERT(decimal(16,2), isnull(ProcessCycleTm,0))) CT
--             	   ,SUM(CASE WHEN ProcessCycleTm !='0'then ProcessCycleTm*1.0   ELSE 0 END) sumCT
--             	   ,sum(CASE WHEN ProcessCycleTm !='0' THEN 1 ELSE 0 END) countCT
            FROM [PVD].[dbo].[View_ProcessData]
            where EventTime>='{eventDate}'
            and EventTime<'{endTime}'
            AND EMT IN (select distinct EMT from [PVD].[dbo].[viewMachineList] 
            WHERE Station like 'WXI_Metal_%' and MachineType='PVD')
            group by EMT,DATEDIFF(MINUTE,'{eventDate}',EventTime)/60)
            SELECT EMT
            ,CASE WHEN maxTotalNum=0 THEN 0
            ELSE CASE WHEN minTotalNum=0 THEN totalCount - 1
            ELSE CASE WHEN maxTotalNum = minTotalNum then 1
            else maxTotalNum + 1 - minTotalNum end END END AS output
            ,'{eventDate}' AS eventtime
            ,hourIndex
--             	   ,CASE WHEN countCT = 0 THEN 0 ELSE Convert(decimal(16,2),1.0*sumCT/countCT) END AS CT
--             	   ,sumCT TotalCT
            FROM ONE
--             ORDER BY CT DESC"""
        # print(f"{eventDate}数据分析中")
        # print("----------------------")
        data = getDataFromSqlS(sql)
        if len(data) == 0:
            continue
        data.columns = columns
        # print("获取数据为：")
        # print(data1)
        pd = pandas.concat([data, pd], axis=0)  # DataFrame 合并
    print(pd)
    if len(pd) != 0:
        insert_intosql(pd, tableName)
    print('output处理完毕')

    path0 = os.path.abspath(os.path.dirname(sys.argv[0]))  # 获取当前执行文件夹路径
    path0 = path0 + "/PVDOEE.ini"
    config = configparser.ConfigParser()
    try:
        config.read(path0, encoding="utf-8-sig")  # 导出配置文件,解决文档格式为utf-8-bom问题
    except Exception as e:
        print(e)
        config.read(path0, encoding="utf-8")  # 导出配置文件
    beginHour = int(config.get("messages", "beginHour"))  # 查询开始时间：多少小时之前
    endHour = int(config.get("messages", "endHour"))  # 查询结束时间：多少小时之前
    tableName = 'PVD_downtime'
    times = conPGSQL(tableName)
    for i in range(endHour - 1, beginHour - 1, -1):
        pd = pandas.DataFrame()
        minHour = i  # 查询开始时间：多少小时之前
        maxHour = i + 1
        eventDate = str(
            (datetime.datetime.now() + datetime.timedelta(hours=minHour)).strftime(f"%Y-%m-%d %H:00:00"))  # 发生的时段
        eventTime = datetime.datetime.strptime(eventDate, f"%Y-%m-%d %H:00:00")
        beginTime = str(
            (datetime.datetime.now() + datetime.timedelta(hours=minHour - 1)).strftime(f"%Y-%m-%d %H:00:00"))  # 时段结束时间
        endTime = str(
            (datetime.datetime.now() + datetime.timedelta(hours=maxHour)).strftime(f"%Y-%m-%d %H:00:00"))  # 时段结束时间
        print('downtime开始时间' + eventDate)
        if eventDate in times:
            print('该时段数据已处理')
            continue

        sql = f"""SELECT distinct [EMT] FROM [PVD].[dbo].[View_MachineStatus] 
        WHERE EMT IN (select distinct EMT from [PVD].[dbo].viewMachineList WHERE Station like 'WXI_Metal_%' and MachineType='PVD')
        AND EventTime > '{eventDate}'
        AND EventTime< '{endTime}'
        GROUP BY EMT"""  # 查询机台号EMT
        machineList = getDataFromSqlS(sql)  # 获取机台号
        # print(machineList)
        sql = f"""SELECT distinct [EMT],[Status],[ErrorCode],[EventTime]
          FROM [PVD].[dbo].[View_MachineStatus]
          WHERE EventTime > '{beginTime}'
          AND EventTime < '{endTime}' 
          and EMT IN (select distinct EMT from [PVD].[dbo].viewMachineList WHERE Station like 'WXI_Metal_%' and MachineType='PVD')
        """
        columns = ['emt', 'status', 'errorcode', 'eventtime']
        rowData = getDataFromSqlS(sql)  # 获取两小时的数据
        if rowData is None or len(rowData) == 0:
            print("No data~")
            continue
        rowData.columns = columns
        # print(rowData)
        print(f"{eventDate}数据分析中")
        for j in range(len(machineList)):  # 遍历机台号计算DOWNTIME
            emt = machineList.loc[j][0]
            emtData0 = rowData.loc[rowData[rowData['emt'] == emt].index]  # 筛选机台号数据
            if (emtData0 is None) or (len(emtData0) == 0):
                continue
            emtData1 = emtData0.loc[emtData0[emtData0['eventtime'] < eventTime].index]  # 筛选第一个小时数据
            emtData11 = emtData1.sort_values(by='eventtime', ascending=False)  # 按时间排倒序
            emtData11.reset_index(drop=True, inplace=True)  # 行号重新排序
            try:
                lastLoop = emtData11.iloc[0, 0:]  # 第一小时最后一条数据
            except:
                lastLoop = None

            emtData2 = emtData0.loc[emtData0[emtData0['eventtime'] >= eventTime].index]  # 筛选第二个小时数据
            if (emtData2 is None) or (len(emtData2) == 0):
                continue
            if lastLoop is not None:
                emtData2 = emtData2.append(lastLoop, ignore_index=True)  # 添加第一小时最后一条数据
            emtData = emtData2.sort_values(by='eventtime', ascending=True)  # 按时间排序
            emtData.reset_index(drop=True, inplace=True)  # 行号重新排序

            print(f"第{j}个EMT:{emt}，EvenTime:{eventDate}")
            # print(emtData)
            if (emtData is None) or (len(emtData) == 0):
                print("No data~")
                continue
            fms = dataDuplicated(emtData, eventTime, endTime)  # 计算每条记录的持续时间
            # print(fms)
            if len(fms) == 0:
                continue
            data = statusCalculate(fms, emt, eventDate)  # 计算每个状态的downtime
            pd = pandas.concat([data, pd], axis=0)  # DataFrame 合并
        if len(pd) != 0:
            print(pd)
            insert_intosql(pd, tableName)
    print('downtime处理完毕')

    path0 = os.path.abspath(os.path.dirname(sys.argv[0]))  # 获取当前执行文件夹路径
    path0 = path0 + "/PVDOEE.ini"
    config = configparser.ConfigParser()
    try:
        config.read(path0, encoding="utf-8-sig")  # 导出配置文件,解决文档格式为utf-8-bom问题
    except Exception as e:
        print(e)
        config.read(path0, encoding="utf-8")  # 导出配置文件
    beginHour = int(config.get("messages", "beginHour"))  # 查询开始时间：多少小时之前
    endHour = int(config.get("messages", "endHour"))  # 查询结束时间：多少小时之前
    tableName = 'PVD_errorcode'
    times = conPGSQL(tableName)
    for i in range(endHour - 1, beginHour - 1, -1):
        pd = pandas.DataFrame()
        minHour = i  # 查询开始时间：多少小时之前
        maxHour = i + 1
        eventDate = str(
            (datetime.datetime.now() + datetime.timedelta(hours=minHour)).strftime(f"%Y-%m-%d %H:00:00"))  # 发生的时段
        eventTime = datetime.datetime.strptime(eventDate, f"%Y-%m-%d %H:00:00")
        beginTime = str(
            (datetime.datetime.now() + datetime.timedelta(hours=minHour - 1)).strftime(f"%Y-%m-%d %H:00:00"))  # 时段结束时间
        endTime = str(
            (datetime.datetime.now() + datetime.timedelta(hours=maxHour)).strftime(f"%Y-%m-%d %H:00:00"))  # 时段结束时间
        print('errorCode开始时间' + eventDate)
        if eventDate in times:
            print('该时段数据已处理')
            continue
        sql = f"""SELECT distinct [EMT] FROM [PVD].[dbo].[View_MachineStatus] WHERE EMT IN 
                (select distinct EMT from [PVD].[dbo].viewMachineList WHERE Station like 'WXI_Metal_%' and MachineType='PVD')
                 AND EventTime > '{eventDate}'
                 AND EventTime< '{endTime}' 
                 GROUP BY EMT"""  # 查询机台号EMT
        machineList = getDataFromSqlS(sql)  # 获取机台号
        sql = f"""SELECT distinct [EMT],[Status],[ErrorCode],[EventTime]
              FROM [PVD].[dbo].[View_MachineStatus]
              WHERE
              EventTime > '{beginTime}'
              AND EventTime < '{endTime}' 
              and EMT IN (select distinct EMT from [PVD].[dbo].viewMachineList WHERE Station like 'WXI_Metal_%' and MachineType='PVD')
                """
        columns = ['emt', 'status', 'errorcode', 'eventtime']
        rowData = getDataFromSqlS(sql)  # 获取两小时的数据
        if rowData is None or len(rowData) == 0:
            print("No data~")
            continue
        rowData.columns = columns
        # print(rowData)
        # print(f"{eventDate}数据分析中")
        for j in range(len(machineList)):  # 遍历机台号计算DOWNTIME
            emt = machineList.loc[j][0]
            emtData0 = rowData.loc[rowData[rowData['emt'] == emt].index]  # 筛选机台号数据
            if (emtData0 is None) or (len(emtData0) == 0):
                continue
            emtData1 = emtData0.loc[emtData0[emtData0['eventtime'] < eventTime].index]  # 筛选第一个小时数据
            emtData11 = emtData1.sort_values(by='eventtime', ascending=False)  # 按时间排倒序
            emtData11.reset_index(drop=True, inplace=True)  # 行号重新排序
            try:
                lastLoop = emtData11.iloc[0, 0:]  # 第一小时最后一条数据
            except:
                lastLoop = None

            emtData2 = emtData0.loc[emtData0[emtData0['eventtime'] >= eventTime].index]  # 筛选第二个小时数据
            if (emtData2 is None) or (len(emtData2) == 0):
                continue
            if lastLoop is not None:
                emtData2 = emtData2.append(lastLoop, ignore_index=True)  # 添加第一小时最后一条数据
            emtData = emtData2.sort_values(by='eventtime', ascending=True)  # 按时间排序
            emtData.reset_index(drop=True, inplace=True)  # 行号重新排序

            # print("----------------------")
            print(f"第{j}个EMT:{emt}，EvenTime:{eventDate}")
            # print(emtData)
            if (emtData is None) or (len(emtData) == 0):
                continue
            fms = dataDuplicated(emtData, eventTime, endTime)  # 计算每条记录的持续时间
            # print(fms)
            if len(fms) == 0:
                continue
            data = errorCodeCalculate(fms, emt, eventDate)  # 计算每个errorcode的downtime
            pd = pandas.concat([data, pd], axis=0)  # DataFrame 合并
        if len(pd) != 0:
            print(pd)
            insert_intosql(pd, tableName)
    print('errorCode处理完毕')

    timer = datetime.datetime.now() - timer
    print('运行时间：' + '%.1f' % (timer.seconds / 60.0) + ' minutes')  # 保留小数点后一位


if __name__ == '__main__':
    runPVDOEE()
