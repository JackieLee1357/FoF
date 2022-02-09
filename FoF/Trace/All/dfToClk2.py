import re
import pandas as pd
from clickhouse_driver import Client

host = 'CNWGPM0HOUSE81'  # 服务器地址
port = 9000  # 端口
user = 'oeuser'  # 用户名
password = 'oeuser123456'  # 密码
database = 'JGPWMD49'  # 数据库
send_receive_timeout = 30  # 超时时间
client = Client(host=host, port=port, user=user, password=password, database=database,
                send_receive_timeout=send_receive_timeout)


def read_sql(sql):
    data, columns = client.execute(
        sql, columnar=True, with_column_types=True)

    df = pd.DataFrame({re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})

    return df


def get_type_dict(tb_name):
    sql = f"select name, type from system.columns where table='{tb_name}';"
    df = read_sql(sql)
    df = df.set_index('name')
    type_dict = df.to_dict('dict')['type']

    return type_dict


def toClick(df, tb_name):
    print(df.columns)
    type_dict = get_type_dict(tb_name)
    columns = list(type_dict.keys())
    dfColumns = list(df.columns)
    newColumns = list(set(dfColumns).difference(set(columns)))  # 求差集
    # if len(newColumns) > 0:  # 获取新列并添加到数据库表中
    #     createColumns(df, tb_name, list(newColumns))
    # 类型处理
    print("1"*20)
    dfColumns = df.columns
    for i in range(len(columns)):
        col_name = columns[i]
        col_type = type_dict[col_name]
        if col_name in dfColumns:
            if 'Date' in col_type:
                df[col_name] = pd.to_datetime(df[col_name])
            elif 'Int' in col_type:
                df[col_name] = df[col_name].astype('int')
            elif 'Float' in col_type:
                df[col_name] = df[col_name].astype('float')
            elif col_type == 'String':
                df[col_name] = df[col_name].astype('str').fillna('')
    # df数据存入clickhouse
    print("2"*20)
    print(df)
    cols = ','.join(dfColumns)
    data = df.to_dict('records')
    client.execute(f"INSERT INTO JGPWMD49.{tb_name} ({cols}) VALUES", data, types_check=True)


def fromClick(sql):
    res = client.execute(sql)
    return res


def createColumns(df, tb_name, cols):  # 添加新列到数据库中
    for i in cols:
        client.execute(f"alter table JGPWMD49.{tb_name}	add column if not exists {i} Nullable(String);")
