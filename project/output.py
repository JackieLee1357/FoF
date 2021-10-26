import datetime
import pymssql
import pandas

##从SQL Server获取数据
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

#去除TotalNum相同的记录
def duplicated(frame):
    row = frame.loc[0][1]
    for i in range(1, len(frame)):
        if row == frame.loc[i][1]:
            frame = frame.drop(i)      #删除行
        else:
            row = frame.loc[i][1]
        print(f"{eventDate}数据清洗中")
        print("===========================")
    frame.reset_index(drop=True, inplace=True)  #行号重新排序
    return frame

#按照EMT号分组并计数
def grouped(fm):
    grouped = fm.groupby(fm[0])  # 按照EMT号分组
    counted = grouped.agg({1: 'count'})  # 按组计数
    counted["日期"] = eventDate  # 新增一列
    counted = counted.reset_index()  # 索引转换为列
    counted.columns = columns  # 修改列名
    return counted

#数据插入SQL Server
def insertIntoSqlS(frame):
    try:
        conn = pymssql.connect(
            host='DESKTOP-5IILHSC',  # 主机名或ip地址
            user='jack',  # 用户名
            password='1485928',  # 密码
            charset='utf8',  # 字符编码
            database='Band OEE')  # 库名
        cursor = conn.cursor()
        for i in range(0, len(frame)):
            emt = frame.iloc[i, 0]
            output = frame.iloc[i, 1]
            eventDate = frame.iloc[i, 2]
            sql = f"INSERT INTO [Band OEE].[dbo].[Output] VALUES({emt}, {output}, CONVERT(DATETIME, {eventDate}, 120))"
            cursor.execute(sql)
            conn.commit()
            print(f"{eventDate}数据写入中")
            print("===========================")
        cursor.close()
        conn.close()
        print("插入SQL成功")
        return
    except pymssql.Error as e:
        print(e)
        return


if __name__ == '__main__':
    day = -2           #查询天数
    pd = pandas.DataFrame()
    for i in range(day,0):
        maxDay = i+1
        minDay = i
        eventDate = str((datetime.datetime.now() + datetime.timedelta(days=minDay)).strftime("%Y-%m-%d"))
        columns = ['EMT', 'Output', 'EventTime']
        sql = f"""SELECT TOP (100) [EMT],[TotalNum],[EventTime] 
        FROM [SMDP].[dbo].[viewSMEAPI_MachineStatusMonth] 
        WHERE EventTime < CONVERT(varchar(10), DATEADD(DAY,{maxDay},GETDATE()), 120)+ ' 06：00：00'     
        AND EventTime > CONVERT(varchar(10), DATEADD(DAY,{minDay},GETDATE()), 120)+ ' 06：00：00'
        AND Station LIKE 'WXI_Metal_%' 
        ORDER BY EMT,EventTime"""
        print(f"{eventDate}数据分析中")
        print("----------------------")
        fm = getDataFromSqlS(sql)
        fm = duplicated(fm)
        data = grouped(fm)
        if i == day:
            pd = data
        else:
            pd = pandas.concat([data, pd], axis=0)
    insertIntoSqlS(pd)
    print(pd)


