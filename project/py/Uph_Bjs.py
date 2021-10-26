import pyodbc
import pandas
import datetime
import configparser
import time
import sqlalchemy
import io
import sqlalchemy.sql.default_comparator

db_url = 'postgres+psycopg2://OE_User:Jabil123456@CNWXIM0WINSVC01:5438/Trace_T1'
# engine = sqlalchemy.create_engine(db_url)
#
config = configparser.ConfigParser()
config.read('c:\\uph_bjs\\uphbjs.ini')

time_start = config.get("messages", "greeting3")
ti = datetime.datetime.strptime(time_start, '%Y-%m-%d %H:%M:%S')
time_next = ti + datetime.timedelta(hours=+1)
time_next_two = config.get("messages", "greeting4")
time_new = time.strftime('%Y-%m-%d %H:00:00')
time_new = datetime.datetime.strptime(time_new, '%Y-%m-%d %H:00:00')
ti_next_two = datetime.datetime.strptime(time_next_two, '%Y-%m-%d %H:%M:%S')
time_nextt_need = ti_next_two + datetime.timedelta(hours=+1)

sql = config.get("messages", "greeting")
sql1 = config.get("messages", "greeting1")
sql2 = config.get("messages", "greeting2")
sql_uph = sql + str(time_start) + sql1 + str(time_next_two) + sql2
wensql = 'SELECT * FROM ('
wensql1 = ")p WHERE P.sneeee LIKE '%nyT%' "
sql_uph1 = wensql + sql_uph + wensql1

print(sql_uph1)

#
#
#
#
#
# def select_sql(sql_uph):
#     conn = pyodbc.connect(r'DRIVER={SQL Server};SERVER=CNWXIM0TRSQLV2A;DATABASE=TRACE;UID=PBIuser;PWD=PBIuser123456;')
#     cur1 = conn.cursor()
#     print(sql_uph)
#     cur1.execute(sql_uph)
#     rows1 = cur1.fetchall()
#     conn.close()
#     print(rows1)
#     print("数据已导出，sql连接已关闭")
#     data_uph = pandas.DataFrame.from_records(rows1, columns=['field', 'bg', 'sn', 'hour', 'date', 'station', 'process'])
#     data_uph['hour'] = data_uph['hour'].astype('int')
#     data_uph['field'] = data_uph['field'].astype('int')
#     db_url = 'postgres+psycopg2://OE_User:Jabil123456@CNWXIM0WINSVC01:5438/Trace_T1'
#     engine = sqlalchemy.create_engine(db_url)
#     connection = engine.raw_connection()
#     cursor = connection.cursor()
#     output = io.StringIO()
#     data_uph.to_csv(output, sep='\t', index=False, header=False)
#     output.seek(0)
#     cursor.copy_from(output, 'uph_bjs', null='')
#     connection.commit()
#     cursor.close()
#     engine.dispose()
#
# #
# #
# #
# #
# #
# #
# def deletesql(delsql,delsql_two,sql):
#     print("删除昨天数据并重新导出一天数据")
#     db_url = 'postgres+psycopg2://OE_User:Jabil123456@CNWXIM0WINSVC01:5438/Trace_T1'
#     engine = sqlalchemy.create_engine(db_url)
#     connection = engine.raw_connection()
#     cur = connection.cursor()
#     cur.execute(delsql)
#     connection.commit()
#     cur.execute(delsql_two)
#     connection.commit()
#     select_sql(sql)
#     connection.commit()
#     cur.close()
#     engine.dispose()
#
# #
# #
# #
# #
# time_two= time.strftime('%Y-%m-%d 06:00:00')
# time_two=datetime.datetime.strptime(time_two,'%Y-%m-%d %H:00:00')
# day_up=time_two+datetime.timedelta(days=-1)
# x=str(day_up)[0:10]
#
# y=str(time_two)[0:10]
#
#
# del_sql="delete  from uph_bjs t where t.date='"
# del_sql2="'and t.hour in ('0','1','2','3','4','5')"
# del_sql3="'and t.hour not in ('0','1','2','3','4','5')"
#
# del_sql_end=del_sql+y+del_sql2
# del_sql_end_two=del_sql+x+del_sql3
# del_sql_end_three=wensql+sql+str(day_up)+sql1+str(time_two)+sql2+wensql1
# print(del_sql_end)
# print(del_sql_end_two)
# print(del_sql_end_three)
#
#
# if time_new==time_two:
# #
# #     print("此次为清空次")
# #     deletesql(del_sql_end,del_sql_end_two,del_sql_end_three)
# #
# # else:
# #     print("此次为正常小时次")
# #     select_sql(sql_uph1)
#
# #
# #
# if time_new>time_next:
#     config.set("messages", "greeting3", str(time_next))
#     config.set("messages", "greeting4", str(time_nextt_need))
#     config.write(open('c:\\UPH_TWO\\UPH_TWO.ini', "r+", encoding="utf-8"))
#
# print('此次已完成，请开始右键运行下一次')

#
#
#
#
#
