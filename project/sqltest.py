import pymssql

def getDataFromSQL():
    dataList = []
    connect = pymssql.connect('localhost', 'jack', '123456', 'Trace')  # 建立连接
    if connect:
        print("连接成功!")

    cursor = connect.cursor()  # 创建一个游标对象,python里的sql语句都要通过cursor来执行
    sql = '''SELECT DISTINCT
        f.process, f.agent, e.IP_Addr as IP,  e.station_id as correctStationID,  f.stationid as errorStationID
    FROM [Trace].[dbo].[JWX_ D52_Agent list -V200820-LiuYang] e inner join
    [Trace].[dbo].[D52 DATA] f
    on e.MAC_Addr=f.agent
    and e.station_id<>f.stationid'''
    cursor.execute(sql)  # 执行sql语句
    row = cursor.fetchone()  # 读取查询结果,
    while row:  # 循环读取所有结果
        row1 = list(row)
        dataList.append(row1)
        row = cursor.fetchone()

    cursor.close()
    connect.close()
    return dataList
