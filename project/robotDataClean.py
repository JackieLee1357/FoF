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

        # sqlStr = f"""
        #     DECLARE @pxh varchar(50)
        #     declare order_cursor cursor  --定义游标
        #     for (select  distinct EMT
        #         from SMDP.dbo.API_CNCCollectionData
        #         WHERE CreateTime > DATEADD(MINUTE, -1, GETDATE())
        #         AND ScanType = 'RobotAllProData'
        #         AND EMT IN (select distinct EMT from viewMachineList WHERE Station like 'WXI_Metal_%' and Type='ROBOT'))  --为游标赋值一个数据集
        #         open order_cursor			--打开游标
        #         fetch next from order_cursor into @pxh --开始循环游标变量（从数据集中拿出第一条数据）
        #         while @@FETCH_STATUS = 0     --返回被 FETCH语句执行的最后游标的状态 （固定写法）
        #         begin
        #             insert into Metal_Robot.db_owner.robot_data
        #             select top (1) ID,
        #             EMT,
        #             MachineName,
        #             json_value(Data, '$.BatteryLife') BatteryLife,
        #             json_value(Data, '$.SerialNumber') SerialNumber,
        #             json_value(Data, '$.PowerOnTime') PowerOnTime,
        #             json_value(Data, '$.ServoOnTime') ServoOnTime,
        #             json_value(Data, '$.AutomationStatus') AutomationStatus,
        #             json_value(Data, '$.ManualStatus') ManualStatus,
        #             json_value(Data, '$.AutomationStatusTime') AutomationStatusTime,
        #             json_value(Data, '$.ManualStatusTime') ManualStatusTime,
        #             json_value(Data, '$.MotorSpeed.Axis1') MotorSpeed,
        #             json_value(Data, '$.ErrorCode') ErrorCode,
        #             json_value(Data, '$.ErrorDesc') ErrorDesc,
        #             json_value(Data, '$.ProcessingProgramVersion') ProcessingProgramVersion,
        #             json_value(Data, '$.TotalNum') TotalNum,
        #             json_value(Data, '$.ProcessCycleTm') ProcessCycleTm,
        #             '{eventHour}',
        #             json_value(Data, '$.Status') Status
        #             from SMDP.dbo.API_CNCCollectionData
        #             WHERE CreateTime > '{eventHour}'
        #             and CreateTime < '{endHour}'
        #             AND ScanType = 'RobotAllProData'
        #             and EMT=@pxh
        #             order by CreateTime desc
        #             fetch next from order_cursor into  @pxh --转到下一个游标(取下条数据集)
        #         end
        #         close order_cursor      -- 关闭游标
        #         deallocate order_cursor   -- 释放游标
        # """

        sqlStr = f"""
                    DECLARE @pxh varchar(50)
                    declare order_cursor cursor  --定义游标
                    for (select distinct EMT from viewMachineList WHERE Station like 'WXI_Metal_%' and Type='ROBOT')  --为游标赋值一个数据集
                        open order_cursor			--打开游标
                        fetch next from order_cursor into @pxh --开始循环游标变量（从数据集中拿出第一条数据）
                        while @@FETCH_STATUS = 0     --返回被 FETCH语句执行的最后游标的状态 （固定写法）
                        begin
                            insert into Metal_Robot.db_owner.robot_data
                            select top (1) ID,
                            EMT,
                            MachineName,
                            json_value(Data, '$.BatteryLife') BatteryLife,
                            json_value(Data, '$.SerialNumber') SerialNumber,
                            json_value(Data, '$.PowerOnTime') PowerOnTime,
                            json_value(Data, '$.ServoOnTime') ServoOnTime,
                            json_value(Data, '$.AutomationStatus') AutomationStatus,
                            json_value(Data, '$.ManualStatus') ManualStatus,
                            json_value(Data, '$.AutomationStatusTime') AutomationStatusTime,
                            json_value(Data, '$.ManualStatusTime') ManualStatusTime,
                            json_value(Data, '$.MotorSpeed.Axis1') MotorSpeed,
                            json_value(Data, '$.ErrorCode') ErrorCode,
                            json_value(Data, '$.ErrorDesc') ErrorDesc,
                            json_value(Data, '$.ProcessingProgramVersion') ProcessingProgramVersion,
                            json_value(Data, '$.TotalNum') TotalNum,
                            json_value(Data, '$.ProcessCycleTm') ProcessCycleTm,
                            '{eventHour}',
                            json_value(Data, '$.Status') Status
                            from SMDP.dbo.API_CNCCollectionData
                            WHERE CreateTime > '{eventHour}'
                            and CreateTime < '{endHour}'
                            AND ScanType = 'RobotAllProData'
                            and EMT=@pxh
                            order by CreateTime desc
                            fetch next from order_cursor into  @pxh --转到下一个游标(取下条数据集)
                        end
                        close order_cursor      -- 关闭游标
                        deallocate order_cursor   -- 释放游标
                """

        sqlStr2 = f'''
                select max(CreateTime) from Metal_Robot.db_owner.robot_data where CreateTime < '{endHour}';
            '''

        deleteSql = """
               delete from Metal_Robot.db_owner.robot_data
               where CreateTime<dateadd(day, -15, getdate())
           """

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




# #
# {"ProcessCycleTm": "0",
#  "RobotModel": "ARC Mate 120iC",
#  "Process": "08/24/2015",
#  "SerialNumber": "LR HandlingTool",
#  "Version": "V8.30P/16",
#  "Status": "1",
#  "ErrorCode": "SYST-034",
#  "ErrorDesc": "SOP/UOP的暂停信号丢失",
#  "AutomationStatus": false,
#  "AutomationStatusTime": "594690",
#  "ManualStatus": true,
#  "ManualStatusTime": "47728",
#  "PowerOnTime": "2257765",
#  "ServoOnTime": "1393687",
#  "EachAxisAngle": {"Axis1": "0", "Axis2": "-35", "Axis3": "4.999999", "Axis4": "0", "Axis5": "-95", "Axis6": "0"},
#  "EachAxisPosition": {"Axis1": "506.9082", "Axis2": "0", "Axis3": "618.9538", "Axis4": "180", "Axis5": "-1.224283E-13",
#                       "Axis6": "-90"},
#  "EachAxisOffset": {"Axis1": "-593.0918", "Axis2": "-250", "Axis3": "-81.0462", "Axis4": "180",
#                     "Axis5": "-1.224283E-13", "Axis6": "-90"},
#  "EachAxisCommand": {"Axis1": "0", "Axis2": "0", "Axis3": "0", "Axis4": "0", "Axis5": "0", "Axis6": "0"},
#  "EachAxisMotorCurrent": {"Axis1": "-0.01098599", "Axis2": "0", "Axis3": "-0.02197199", "Axis4": "0", "Axis5": "0",
#                           "Axis6": "0"},
#  "EachAxisMotorCurrentCommand": {"Axis1": "0", "Axis2": "0", "Axis3": "0", "Axis4": "0", "Axis5": "0", "Axis6": "0"},
#  "ToolFrontSpeed": "",
#  "ToolFrontSpeedCommand": "",
#  "MotorSpeed": {"Axis1": "85", "Axis2": "85", "Axis3": "85", "Axis4": "85", "Axis5": "85", "Axis6": "85"},
#  "MotorTorque": {"Axis1": "0", "Axis2": "0", "Axis3": "0", "Axis4": "0", "Axis5": "0", "Axis6": "0"},
#  "EachAxisLoad": {"Axis1": "3", "Axis2": "2", "Axis3": "5", "Axis4": "3", "Axis5": "1", "Axis6": "0"},
#  "BatteryLife": "Good",
#  "TotalNum": 0,
#  "ProcessingProgramNumber": "",
#  "ProcessingProgramVersion": "",
#  "EventTime": "2021-08-29 09:31:32",
#  "ProductInfo": ""}
# #