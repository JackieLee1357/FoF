import pymssql


conn = pymssql.connect(
        host='WIN-TSFJJJ8FOKJ\SQLEXPRESS',  # 主机名或ip地址      WIN-TSFJJJ8FOKJ\SQLEXPRESS     DESKTOP-5IILHSC
        user='jack',  # 用户名
        password='1485928',  # 密码
        charset='utf8',  # 字符编码
        database='Band OEE')  # 库名
cursor = conn.cursor()
print("链接SQL成功")

cursor.close()
conn.close()