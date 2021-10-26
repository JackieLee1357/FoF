import pymssql

def addressPool(segment):     #获取对应网段的地址池
    addressPool1 = []
    lastNumber = 2
    while lastNumber < 255:
        addressValue = '172.23.' + str(segment) + '.' + str(lastNumber)
        addressPool1.append(addressValue)
        lastNumber += 1
    return addressPool1

def saveToSql(dataList, building):          #把地址池导入数据库中
    connect = pymssql.connect('localhost', 'jack', '123456', 'Trace', autocommit=True)  # 建立连接
    if connect:
        print("SQL连接成功!")
    cursor = connect.cursor()  # 创建一个游标对象,python里的sql语句都要通过cursor来执行
    tableName = building + "AddressPool"
    sqlCreate = '''use Trace
    CREATE TABLE %s(
    serial  int  not null primary key,     
	building  varchar(8) null,
	process  varchar(20)  null,
	address  varchar(14)  not null,
	stationID nvarchar(50) null)''' % tableName   # 新建地址池表
    try:
        cursor.execute(sqlCreate)  # 执行sql语句
        serial = 2
        for each in dataList:
            sqlInsert = '''insert into dbo.%s (serial, address) values ('%d', '%s')''' % (tableName, serial, each)
            serial += 1
            try:
                cursor.execute(sqlInsert)
            except Exception as e:
                connect.rollback()
                # print('插入数据失败')
                # print(e)
    except:
        print('该地址池的表已建立')
    cursor.close()
    connect.close()

def getAddressFromSql(building, process):
    connect = pymssql.connect('localhost', 'jack', '123456', 'Trace', autocommit=True)  # 建立连接
    cursor = connect.cursor()  # 创建一个游标对象,python里的sql语句都要通过cursor来执行
    tableName = building + "AddressPool"
    sqlQuery = '''SELECT TOP (1) address FROM %s WHERE building='%s' AND process='%s' ''' % (tableName, building, process)
    try:
        cursor.execute(sqlQuery)
        addressSelect = cursor.fetchone()[0]
        sqlDelete = '''DELETE FROM %s WHERE address='%s' ''' % (tableName, addressSelect)
        try:
            cursor.execute(sqlDelete)
        except Exception as e:
            connect.rollback()
            print('删除数据失败')
            print(e)
    except Exception as e:
        connect.rollback()
        print('查询数据失败')
        print(e)
    cursor.close()
    connect.close()
    return addressSelect

if __name__ == '__main__':
    AEAddressPool = addressPool(207)
    saveToSql(AEAddressPool, "AE")
    print(getAddressFromSql("AE", "BG"))
