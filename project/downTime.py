import datetime
import pymssql
from pymssql import _mssql
from pymssql import _pymssql  # 解决打包后exe运行报错
import uuid
import decimal
import pandas
import sqlalchemy
import configparser
import psycopg2
from sqlalchemy import create_engine
from io import StringIO
import io


# 从SQL Server获取数据
def getDataFromSqlS(sql):
    try:
        conn = pymssql.connect(
            host='CNWXIM0TRSQLV4A',  # 主机名或ip地址
            user='PBIuser',  # 用户名
            password='PBIUser123456',  # 密码
            charset='utf8',  # 字符编码
            database='SMDP')  # 库名
        cursor = conn.cursor()
        cursor.execute(sql)
        df = pandas.DataFrame(cursor.fetchall())
        cursor.close()
        conn.close()
        return df
    except pymssql.Error as e:
        print(e)
        return None


# 按照EMT号分组并计数'[beginHour ：开始计算的时段，当前时间之前xx小时]'
# '[endHour ：结束计算的时段，当前时间之前xx小时]'
def grouped(fm):
    grouped = fm.groupby(fm[0])  # 按照EMT号分组
    counted = grouped.agg({2: 'count'})  # 按组计数
    counted["日期"] = eventDate  # 新增一列
    counted = counted.reset_index()  # 索引转换为列
    counted.columns = columns  # 修改列名
    return counted


# 去除未报警的记录
def duplicated(frame):
    lenth = len(frame)
    i = lenth - 1
    print(f"{eventDate}数据清洗中")
    while i > 0:
        # print("===========================")
        row = frame.loc[i][1]
        if (row == '3') & (frame.loc[i - 1][1] != '3'):
            i -= 1
            continue
        elif (row != '3') & (frame.loc[i - 1][1] == '3'):
            i -= 1
            continue
        else:
            frame = frame.drop(i)
            i -= 1
    print(frame)
    if frame.loc[0][1] != '3':
        print('删除第一行状态不为3')
        frame = frame.drop(0)
    elif frame.loc[0][1] == '3':
        print('第一行状态为3')
        frame.loc[0] = [frame.loc[0][0], frame.loc[0][1], frame.loc[0][2], pandas.to_datetime(eventDate)]  # 修改Frame数据
    print(frame)
    frame.reset_index(drop=True, inplace=True)  # 行号重新排序
    print(frame)
    if len(frame) != 0:
        if frame.loc[len(frame) - 1][1] == '3':
            print('最后一行状态为3')
            frame.loc[len(frame)] = [frame.loc[len(frame) - 1][0], '1', frame.loc[len(frame) - 1][2],
                                     pandas.to_datetime(endTime)]  # 最后面新增一行数据
    print('数据清洗完毕')
    return frame


# 计算downtime
def calculate(fm):
    downtime = fm.loc[0][3] - fm.loc[0][3]
    for i in range(0, len(fm), 2):
        downtime += fm.loc[i + 1][3] - fm.loc[i][3]
    downtime = str(downtime.seconds)
    dt = pandas.DataFrame([[fm.loc[0][0], downtime, eventDate]])
    # print(dt)
    dt.columns = columns  # 修改列名
    print("===========================")
    return dt


# 数据插入PG sql
def insert_intosql(frame, tableName):
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    frame.to_sql(tableName, engine, index=False, if_exists='append')
    engine.dispose()
    print('数据插入PG SQL成功')


def conPGSQL(tableName):
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute(f'select distinct eventtime from {tableName} ORDER BY eventtime DESC;')
    row = cursor.fetchall()
    connection.commit()
    cursor.close()
    engine.dispose()
    return row


if __name__ == '__main__':
    timer = datetime.datetime.now()
    config = configparser.ConfigParser()
    config.read('C:\\BandOEE\\bandoee.ini')  # 导出配置文件
    beginHour = int(config.get("messages", "beginHour"))  # 查询开始时间：多少小时之前
    endHour = int(config.get("messages", "endHour"))  # 查询结束时间：多少小时之前
    pd = pandas.DataFrame()
    for i in range(endHour - 1, beginHour - 1, -1):
        times = conPGSQL('bandoee_outputtemp')
        print(times)
        maxTime = i + 1
        minTime = i
        eventDate = str((datetime.datetime.now() + datetime.timedelta(hours=minTime)).strftime(f"%Y-%m-%d %H:00:00"))  #
        print('开始时间' + eventDate)
        if len(times) != 0:
            try:
                if eventDate == times[-maxTime][0]:
                    print('该时段数据已处理')
                    continue
            except Exception as e:
                print(e)
        columns = ['emt', 'output', 'eventtime', 'hourindex', 'ct', 'totalct']
        sql = f"""WITH ONE AS(
        SELECT EMT
               ,DATEDIFF(MINUTE,'{eventDate}',EventTime)/60+1 hourIndex
        	   ,max(TotalNum) maxTotalNum
        	   ,min(TotalNum) minTotalNum
        	   ,COUNT(TotalNum) totalCount
        	   ,avg( CONVERT(decimal(16,2), isnull(ProcessCycleTm,0))) CT
        	   ,SUM(CASE WHEN ProcessCycleTm !='0'then ProcessCycleTm*1.0   ELSE 0 END) sumCT
        	   ,sum(CASE WHEN ProcessCycleTm !='0' THEN 1 ELSE 0 END) countCT
        FROM [SMDP].[dbo].[viewSMEAPI_CNCProductInfoMonth] 
        where EventTime<'{eventDate}'
        and EventTime>=DATEADD(HOUR,-1,'{eventDate}') 
        AND Station LIKE 'WXI_Metal_%'
        group by EMT,DATEDIFF(MINUTE,'{eventDate}',EventTime)/60)
        SELECT EMT
               ,CASE WHEN maxTotalNum=0 THEN 0 ELSE CASE WHEN minTotalNum=0 THEN totalCount - 1 ELSE CASE WHEN maxTotalNum = minTotalNum then 1 else maxTotalNum + 1 - minTotalNum end END END AS output
        	   ,'{eventDate}' AS eventtime
               ,hourIndex
        	   ,CASE WHEN countCT = 0 THEN 0 ELSE Convert(decimal(16,2),1.0*sumCT/countCT) END AS CT
        	   ,sumCT TotalCT   
        FROM ONE
        ORDER BY CT DESC"""
        print(f"{eventDate}数据分析中")
        print("----------------------")
        data = getDataFromSqlS(sql)
        data.columns = columns
        pd = pandas.concat([data, pd], axis=0)  # DataFrame 合并
    print(pd)
    insert_intosql(pd, 'bandoee_outputtemp')
    print('output处理完毕')

    config = configparser.ConfigParser()
    config.read('C:\\BandOEE\\bandoee.ini')  # 导出配置文件
    beginHour = int(config.get("messages", "beginHour"))  # 查询开始时间：多少小时之前
    endHour = int(config.get("messages", "endHour"))  # 查询结束时间：多少小时之前
    for i in range(endHour - 1, beginHour - 1, -1):
        times = conPGSQL('bandoee_downtime')
        print(times)
        pd = pandas.DataFrame()
        minHour = i  # 查询开始时间：多少小时之前
        maxHour = i + 1
        eventDate = str(
            (datetime.datetime.now() + datetime.timedelta(hours=minHour)).strftime(f"%Y-%m-%d %H:00:00"))  # 发生的时段
        endTime = str(
            (datetime.datetime.now() + datetime.timedelta(hours=maxHour)).strftime(f"%Y-%m-%d %H:00:00"))  # 时段结束时间
        print('开始时间' + eventDate)
        if len(times) != 0:
            try:
                if eventDate == times[-maxHour][0]:
                    print('该时段数据已处理')
                    continue
            except Exception as e:
                print(e)
        columns = ['emt', 'downtime', 'eventtime']
        sql = f"""SELECT [EMT] FROM [SMDP].[dbo].[viewSMEAPI_MachineStatus] WHERE Station LIKE 'WXI_Metal_%'
        AND EventTime > CONVERT(VARCHAR(14),DATEADD(HH,{minHour},GETDATE()),120)+'00：00'
        AND EventTime< CONVERT(VARCHAR(14),DATEADD(HH,{maxHour},GETDATE()),120)+'00：00'   
        GROUP BY EMT"""  # 查询机台号EMT
        machineList = getDataFromSqlS(sql)
        print(f"{eventDate}数据分析中")
        for j in range(len(machineList)):  # 遍历机台号计算DOWNTIME
            emt = machineList.loc[j][0]
            sql = f"""SELECT [EMT],[Status],[TotalNum],[EventTime] FROM
            (SELECT [EMT],[Status],[TotalNum],[EventTime]
              FROM [SMDP].[dbo].[viewSMEAPI_MachineStatus]
              WHERE Station LIKE 'WXI_Metal_%'
              AND EventTime > CONVERT(VARCHAR(14),DATEADD(HH,{minHour},GETDATE()),120)+'00：00'
              AND EventTime< CONVERT(VARCHAR(14),DATEADD(HH,{maxHour},GETDATE()),120)+'00：00'
              AND EMT = '{emt}'
              UNION ALL
              SELECT TOP (1) [EMT],[Status],[TotalNum],[EventTime]
              FROM [SMDP].[dbo].[viewSMEAPI_MachineStatus]
              WHERE Station LIKE 'WXI_Metal_%'
              AND EventTime > CONVERT(VARCHAR(14),DATEADD(HH,{minHour}-1,GETDATE()),120)+'00：00'
              AND EventTime< CONVERT(VARCHAR(14),DATEADD(HH,{maxHour}-1,GETDATE()),120)+'00：00'
              AND EMT = '{emt}') A
            ORDER BY EMT ,EventTime"""
            print("----------------------")
            fm = getDataFromSqlS(sql)  # 从V4A数据库获取数据
            fm = duplicated(fm)  # 数据清洗
            if len(fm) == 0:
                continue
            data = calculate(fm)  # 计算downtime
            pd = pandas.concat([data, pd], axis=0)  # DataFrame 合并
        insert_intosql(pd, 'bandoee_downtime')
    timer = datetime.datetime.now() - timer
    print('downtime处理完毕')
    print('运行时间：' + str(timer))
