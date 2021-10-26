import datetime
import configparser
import time
import pyodbc
import pandas
import xlwt

today = datetime.date.today()
now = datetime.datetime.now()
add_hour1 = datetime.date.today()+datetime.timedelta(days=-1) #时间1
print(add_hour1)
add_hour = datetime.date.today()+datetime.timedelta(hours=1,minutes=00,seconds=00)
ti1 =time.strftime('%Y-%m-%d %H:00:00')
print(ti1)#时间2
ti2=str(ti1)
#转换
ti3 = datetime.datetime.strptime(ti1,'%Y-%m-%d %H:%M:%S')
time4=ti3+datetime.timedelta(days=-1)
print(time4)#时间4
sju=add_hour1.strftime('%Y%m%d')
sju1='API_WIPData'+sju
print(sju1)
#add_hour1 =时间1，ti1=时间2，time4=时间4 sju=需要查询的表

config = configparser.ConfigParser()
config.read('c:\\pk\\tsxt.ini')
a=config.get("messages", "greeting")
b=config.get("messages", "greeting1")
c=config.get("messages", "greeting2")
d=config.get("messages", "greeting3")
e=config.get("messages", "greeting4")

sql=a+str(add_hour1)+b+str(ti1)+c+sju+" "+d+str(time4)+e
print(sql)

conn = pyodbc.connect(r'DRIVER={SQL Server};SERVER=CNWXIM0TRSQLV2A;DATABASE=OEE;UID=OE_USER;PWD=Jabil156318111;')
cur = conn.cursor()
cur.execute(sql)

rows=cur.fetchall()
print(rows)
conn.close()

workbook = xlwt.Workbook()  # 创建workbook 对象
file_name = datetime.datetime.now().strftime('%y%m%d%H%M') + '.csv'
print(file_name)

#datefm = pandas.DataFrame(rows)
drt=pandas.DataFrame.from_records(list(rows))
#datefm.to_csv(file_name,index=0,header=0)
drt.to_csv(file_name,index=0,header=0)
time.sleep(10)










