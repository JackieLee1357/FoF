import pymssql
import pandas

def get_conn():
    try:
        conn = pymssql.connect(
            host='CNWXIM0TRSQLV4A',  # 主机名或ip地址
            user='PBIuser',  # 用户名
            password='PBIUser123456',  # 密码
            charset='utf8',  # 字符编码
            database='SMDP')  # 库名
        cursor=conn.cursor()
        cursor.execute("select * from [SMDP].[dbo].[viewMachineList]")
        data=pandas.DataFrame(cursor.fetchall())
        cursor.close()
        conn.close()
        return data
    except pymssql.Error as e:
        print(e)
        return 0


# SQL语句
SQL1 = '''
select * from viewMachineList
'''

if __name__ == '__main__':
    data = get_conn()
    print(data.loc[[1,1]])
    print(data.loc[6])
    print(data.loc[6][15])