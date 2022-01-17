import datetime
import pymssql
import pandas
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
import configparser
from io import StringIO
import io

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

#按照EMT号分组并计数
def grouped(fm):
    grouped = fm.groupby(fm[0])  # 按照EMT号分组
    counted = grouped.agg({2: 'count'})  # 按组计数
    counted["日期"] = eventDate  # 新增一列
    counted = counted.reset_index()  # 索引转换为列
    counted.columns = columns  # 修改列名
    return counted

def insert_intosql(pagedata):
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    output = io.StringIO()
    pagedata.to_csv(output, sep='\t', index=False, header=False)
    output1 = output.getvalue()
    connection = connection
    cursor = connection.cursor()
    cursor.copy_from(StringIO(output1), 'BandOEE_output') #, columns=['EMT', 'output', 'eventtime'])
    cursor.execute('select * from BandOEE_output')
    row = cursor.fetchall()
    connection.commit()
    cursor.close()
    engine.dispose()
    print('sql插入完成')
    return row

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('C:\\BandOEE\\bandoee.ini')  # 导出配置文件
    beginHour = int(config.get("messages", "beginHour"))  # 查询开始时间：多少小时之前
    endHour = int(config.get("messages", "endHour"))  # 查询结束时间：多少小时之前
    pd = pandas.DataFrame()
    for i in range(beginHour, endHour):
        maxTime = i+1
        minTime = i
        eventDate = str((datetime.datetime.now() + datetime.timedelta(hours=minTime)).strftime(f"%Y-%m-%d %H:00:00"))  #
        columns = ['EMT', 'output', 'eventtime', 'hourIndex', 'CT', 'totalCT']
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
        #print(sql)
        print(f"{eventDate}数据分析中")
        print("----------------------")
        data = getDataFromSqlS(sql)
        #data1 = grouped(fm)
        pd = pandas.concat([data, pd], axis=0)    #DataFrame 合并
    print(pd)
    #row = insert_intosql(pd)
    #print(row)


