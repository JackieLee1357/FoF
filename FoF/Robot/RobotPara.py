#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: robotDataClean.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site:
# @Time: 8月 20, 2021
# ---
import configparser
import datetime
import os
import sys
import sqlalchemy
import pandas
import pymssql


def conSqlS(sql):
    conn = pymssql.connect(
        host='CNWXIM0TRSQLV4A',  # 主机名或ip地址
        user='PBIuser',  # 用户名
        password='PBIUser123456',  # 密码
        charset='utf8',  # 字符编码
        database='SMDP')  # 库名
    cursor = conn.cursor()
    cursor.execute(sql)
    try:
        df = pandas.DataFrame(cursor.fetchall())
        print("数据读取完毕~")
    except:
        df = None
        print("数据操作完毕~")
    conn.commit()
    cursor.close()
    conn.close()
    return df


if __name__ == "__main__":
    time1 = datetime.datetime.now()
    path0 = os.path.abspath(os.path.dirname(sys.argv[0]))  # 获取当前执行文件夹路径
    path0 = path0 + "/robotoee.ini"
    config = configparser.ConfigParser()
    config.read(path0, encoding='utf-8-sig')  # 导出配置文件
    beginHour = int(config.get("messages", "beginHour"))  # 查询开始时间：多少小时之前
    endHour = int(config.get("messages", "endHour"))  # 查询结束时间：多少小时之前
    pandas.set_option('display.max_columns', None)  # 显示所有列

    for i in range(endHour - 1, beginHour - 1, -1):
        eventHour = str(
            (datetime.datetime.now() + datetime.timedelta(hours=i)).strftime(f"%Y-%m-%d %H:00:00"))  # 发生时段
        endHour = str(
            (datetime.datetime.now() + datetime.timedelta(hours=i + 1)).strftime(f"%Y-%m-%d %H:00:00"))  # 发生时段
        print('开始时间' + eventHour)


        sqlStr = f"""WITH CTEProcess AS (
            SELECT *
            FROM [dbo].viewCollectCncDataPIS_2day x with (nolock)
            where x.DataType = 'RobotAllProData'
            and x.CreateTime = (
            SELECT MAX(CreateTime) AS Expr1 FROM dbo.viewCollectCncDataPIS_2day AS y WITH (nolock)
            WHERE (x.EMT = y.EMT) AND Y.CreateTime >= '{eventHour}' and CreateTime < '{endHour}')
            )
            insert into Metal_Robot.db_owner.robot_data
            SELECT a.SequentialNumber ID,
                   a.EMT,
                   b.Name MachineName,
                   json_value(a.AllData, '$.BatteryLife') BatteryLife,
                    json_value(a.AllData, '$.SerialNumber') SerialNumber,
                    json_value(a.AllData, '$.PowerOnTime') PowerOnTime,
                    json_value(a.AllData, '$.ServoOnTime') ServoOnTime,
                    json_value(a.AllData, '$.AutomationStatus') AutomationStatus,
                    json_value(a.AllData, '$.ManualStatus') ManualStatus,
                    json_value(a.AllData, '$.AutomationStatusTime') AutomationStatusTime,
                    json_value(a.AllData, '$.ManualStatusTime') ManualStatusTime,
                    json_value(a.AllData, '$.MotorSpeed.Axis1') MotorSpeed,
                    json_value(a.AllData, '$.ErrorCode') ErrorCode,
                    json_value(a.AllData, '$.ErrorDesc') ErrorDesc,
                    json_value(a.AllData, '$.ProcessingProgramVersion') ProcessingProgramVersion,
                    json_value(a.AllData, '$.TotalNum') TotalNum,
                    json_value(a.AllData, '$.ProcessCycleTm') ProcessCycleTm,
                    '{eventHour}' as CreateTime,
                    json_value(a.AllData, '$.Status') Status
            from CTEProcess a
            inner join viewMachineList b on b.EMT = a.EMT
            Where b.Station like 'WXI_Metal_%'
        """

        sqlStr2 = f'''
                select max(CreateTime) from Metal_Robot.db_owner.robot_data where CreateTime < '{endHour}';
            '''

        deleteSql = """
               delete from Metal_Robot.db_owner.robot_data
               where CreateTime<dateadd(day, -15, getdate())
           """

        # print(sqlStr)
        lastTime = conSqlS(sqlStr2)
        try:
            lastTime = lastTime.iloc[0, 0].strftime(f"%Y-%m-%d %H:00:00")
        except:
            lastTime = '2021-08-01 00:00:00'
        if lastTime < eventHour:
            dataSource = conSqlS(sqlStr)
            print('数据插入数据库完毕~')
            conSqlS(deleteSql)
            print('定时删除数据完毕~')
        else:
            print("该时段数据已处理~")
            dataSource = []

    time2 = datetime.datetime.now()
    print(f"共用时{time2 - time1}")

