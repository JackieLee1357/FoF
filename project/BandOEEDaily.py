import json
import pandas
import time
import datetime
import urllib.request
import uuid
import decimal
import sqlalchemy
import configparser

def get_record(url):                    #从URL获取Json文件
    resp = urllib.request.urlopen(url)
    res = resp.read()
    ele_json = json.loads(res)          #Json转化为字典
    print('获取数据成功')
    return ele_json

def convertToPandas(data):               #字典转为DateFrame
    data = data['data1']
    columns = ["product_date", "machineno", "project", "line", "station", "emt", "oeevalue"]
    dateframe = pandas.DataFrame(columns=columns)
    for i in range(len(data)):
        row = list(data[i].values())
        dateframe.loc[i] = row
    print('转换数据成功')
    return dateframe

def date_compare(date1, date2, fmt) -> bool:         #日期大小比较
    zero = datetime.datetime.fromtimestamp(0)
    try:
        d1 = datetime.datetime.strptime(date1, fmt)
    except:
        d1 = zero
    try:
        d2 = datetime.datetime.strptime(date2, fmt)
    except:
        d2 = zero
    print('日期比较成功')
    return d1 > d2

# DataFrame数据插入PG sql
def insert_intosql(frame, tableName):
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    print('链接PG SQL成功')
    frame.to_sql(tableName, engine, index=False, if_exists='append')
    engine.dispose()
    print('数据插入PG SQL成功')

def conPGSQL(tableName):             #链接PG数据库，查询已处理的时段
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute(f'select distinct product_date from {tableName};')
    row = cursor.fetchall()
    row =  [''.join(i) for i in row]    #元组转化为列表
    connection.commit()
    cursor.close()
    engine.dispose()
    return row

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('C:\\BandOEE\\bandoee.ini')  # 导出配置文件
    beginDay = int(config.get("messages", "beginDay"))  # 查询开始日期：多少天之前
    endDay = int(config.get("messages", "endDay"))  # 查询结束时间：多少天之前
    tableName = 'bandoee_oeedaily'
    date = str((datetime.datetime.now() + datetime.timedelta(days=endDay)).strftime("%Y-%m-%d"))[:10]     #截至日期
    date2 = str((datetime.datetime.now() + datetime.timedelta(days=beginDay)).strftime("%Y-%m-%d"))[:10]    #开始日期
    while date_compare(date,date2,"%Y-%m-%d"):
        times = conPGSQL(tableName)
        dateTmp = date + 'T00:00:00'
        print(times)
        print('日期为：' + dateTmp)
        if dateTmp in times:
            print('该时段数据已处理')
            date = (datetime.datetime.strptime(date, "%Y-%m-%d") + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")[:10]
            continue
        url = f"http://cnwxim0kaba01.corp.jabil.org:8081/pis_m_api/api/OEEReport/GetMachineDailyOeeAPI?Plant=WUXI_M&BG=WUXI%20Metal&Project&Line&Station&QueryDate={str(date)}"
        jsonData = get_record(url)
        frameData = convertToPandas(jsonData)
        insert_intosql(frameData, tableName)
        date = (datetime.datetime.strptime(date, "%Y-%m-%d")+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")[:10]