#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: dataTransfer.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 8月 21, 2021
# ---
import pandas
import pymssql
import sqlalchemy
import psycopg2
import pyodbc



def conPGSQL(sqlData):  # 链接PG数据库，查询已处理的时段
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute(sqlData)
    row = cursor.fetchall()
    # row =  [''.join(i) for i in row]    #元组转化为列表
    connection.commit()
    cursor.close()
    engine.dispose()
    print('------------')
    return row


# def conSqlS(sql):
#     # try:
#     conn = pymssql.connect(
#         host='CNWXIM0TRSQLV4A',  # 主机名或ip地址
#         user='PBIuser',  # 用户名
#         password='PBIUser123456',  # 密码
#         charset='UTF-8',  # 字符编码
#         database='Metal_Robot')  # 库名
#     cursor = conn.cursor()
#     cursor.execute(sql)
#     conn.commit()
#     cursor.close()
#     conn.close()
#     print('插入完成~')
#     return
#     # except pymssql.Error as e:
#     #     print(e)
#     #     return None


def insert_intosql(frame, tableName):
    # 数据插入PG sql
    db_url = 'mssql+pymssql://PBIuser:PBIUser123456@CNWXIM0TRSQLV4A/Metal_Robot'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    frame.to_sql(tableName, engine, index=False, if_exists='append')
    engine.dispose()
    print('数据写入SQL Server成功')
    print('------------')


if __name__ == '__main__':
    sql = '''select *
            from robot_output;'''
    pgData = conPGSQL(sql)
    data = pandas.DataFrame(pgData)
    pandas.to_datetime(data.iloc[0:, 2])
    pandas.to_numeric(data.iloc[0:, 1])
    pandas.to_numeric(data.iloc[0:, 3])
    pandas.to_numeric(data.iloc[0:, 4])
    pandas.to_numeric(data.iloc[0:, 5])

    print(data)
    insert_intosql(data, 'robot_output')

    str = "TBD"
    # for i in range(len(data1)):
    #     #data1.iloc[i, 6] = data1.iloc[i, 6].encode('gbk').decode('gb18030')
    #     sql = f'''insert into robot_output (emt, output, eventtime, hourindex, ct, totalct)
    #                 values ('{data1.iloc[i, 0]}', {data1.iloc[i, 1]}, '{data1.iloc[i, 2]}', {data1.iloc[i, 3]},
    #                  {data1.iloc[i, 4]},  {data1.iloc[i, 5]}); '''
    #     print(sql)
    #     conSqlS(sql)

        #









