#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: actionTracker2.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site:
# @Time: 8月 21, 2021
# ---

import base64
import configparser
import datetime
import os
import smtplib
import sys
import time
import traceback
from email.header import Header
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from importlib import reload
import matplotlib
import pandas
import numpy as np
import pandas as pd
import pymssql
import sqlalchemy
import sqlalchemy.sql.default_comparator
from matplotlib import pyplot as plt, ticker

stdi, stdo, stde = sys.stdin, sys.stdout, sys.stderr
reload(sys)  # 通过import引用进来时,setdefaultencoding函数在被系统调用后被删除了，所以必须reload一次
sys.stdin, sys.stdout, sys.stderr = stdi, stdo, stde


def readDataSource(pathName):
    # 读取数据源excel，进行数据处理，并返回DataFrame
    sheetKeys = pandas.read_excel(pathName, sheet_name=None)  # 读取excel
    pandas.set_option('display.max_columns', None)  # 显示所有列
    pandas.set_option('display.max_rows', None)  # 显示所有行
    sheetData = pandas.read_excel(pathName, sheet_name=list(sheetKeys).index('数据'))
    sheetData.columns = range(sheetData.shape[1])
    # print(sheetData.shape[1])
    # print(len(list(sheetData.values[1])))
    # print(list(sheetData.values[1]))
    index = list(sheetData.values[1]).index(dateDays)  # 当前日期的index
    print('--------')
    sheetData.drop(sheetData.columns[list(range(int(index) + 5, sheetData.shape[1]))], axis=1, inplace=True)  # 去掉多余数据
    sheetData.drop(sheetData.columns[list(range(6, int(index)))], axis=1, inplace=True)  # 去掉多余数据
    for i in range(len(sheetData)):
        if sheetData.loc[i, 0] == '制程':
            sheetData = sheetData.loc[i + 2:]
            break
    sheetData.loc[0:, '日期'] = dateNow
    sheetData.loc[0:, 'project'] = project
    sheetDataTableName = ['制程', '站别', '被卡控站点', '漏失目标', 'dri', '呈现工站', '总数', '漏失数量', '漏失率',
                          '问题点描述', '问题类型', '日期', 'project']
    sheetData.columns = sheetDataTableName
    print('源数据读取完毕')
    print('------------')
    return sheetData


def dataSourceSplit(sheetData):
    # 把数据分成三部分，FACA表和TraceMissing表
    OQCStation = sheetData.loc[sheetData[sheetData['漏失目标'] == 0.002].index]  # 获取漏失率为0.2%的行
    OQCStation = OQCStation[OQCStation['漏失率'] < 0.002]  # 筛选漏失率低于0.2%的行
    dfList = sheetData.drop(OQCStation.index)  # 去掉漏失率低于0.2%的行
    dfList = dfList.sort_values(by='漏失率', ascending=False)  # Phoenix 按漏失率排序
    # dfList = dfList.iloc[0:5, ]  # 获取漏失率前5项
    dfList = dfList[dfList['漏失率'] > 0.001]  # 获取漏失率>0.001的行

    dfList.reset_index(drop=True, inplace=True)  # 行号重新排序
    if sheetData.empty:  # 判断DataFrame是否为空
        print('今日数据未更新，请更新数据！')
        sys.exit(0)
    elif dfList.empty:
        print('今日无漏失数据！')
        global dataIsNone  # 修改主程序变量，变量作用域
        dataIsNone = True
        return [None, None, sheetData]
    print('今日漏失数据为：')
    print(dfList)
    projectLen = len(project)
    sqlData2 = f"""
        select max(convert(int,right(item_id,len(item_id)-{projectLen})))
        from actiontracker_tracemissing
        WHERE item_id like '{project}%' ;
    """
    maxId = conSqlS(sqlData2)[0][0]
    if maxId is None:
        maxId = 1
    # print(maxId)
    traceMissingTable = dfList
    traceMissingTable["fa"] = "待回复"
    traceMissingTable["ca"] = "待回复"
    traceMissingTable["item_id"] = [project+str(maxId+i+1) for i in range(len(dfList))]
    traceMissingTableName = ['item_id', '日期', '被卡控站点', '站别', '漏失数量', '漏失率', 'dri', 'fa',
                             'ca']  # , 'returndri', 'returntime']
    traceMissingTable = traceMissingTable.loc[0:, traceMissingTableName]  # 选取特定列
    traceMissingTable.columns = ['item_id', '日期', '被卡控站点', '呈现站点', '漏失数量', '漏失率', 'dri', 'fa',
                             'ca']
    traceMissingTable.loc[0:, 'returndri'] = '待回复'  # 新增一列
    traceMissingTable = traceMissingTable.fillna(value='待回复')  # 替换NaN值
    traceMissingTable.loc[0:, 'returntime'] = None  # 新增一列
    try:
        pandas.to_datetime(traceMissingTable.loc[0:, '日期'], format='%Y-%m-%d')
        pandas.to_datetime(traceMissingTable.loc[0:, 'returntime'])
    except Exception as e:
        print(e)
        print("excel源文件日期格式错误")
        sys.exit(1)
    try:
        pandas.to_numeric(traceMissingTable.loc[0:, '漏失数量'])
        pandas.to_numeric(traceMissingTable.loc[0:, '漏失率'])
    except Exception as e:
        print(e)
        print("excel源文件数字格式错误，请检查站点名称是否错误~")
        sys.exit(1)

    FACATableName = ['item_id', '日期']
    FACATable = traceMissingTable.loc[0:, FACATableName]  # 选取特定列
    # FACATable.loc[0:, 'id'] = [maxId+i+1 for i in range(len(FACATable))]  # 新增一列
    FACATable.loc[0:, 'project'] = 'TraceMissing'  # 新增一列
    FACATable = FACATable.loc[0:, ['item_id', 'project', '日期']]  # 选取特定列,重新排序
    FACATable.columns = ['item_id', 'project', 'eventtime']
    FACATable = FACATable.fillna(value='待回复')  # 替换NaN值
    pandas.to_datetime(FACATable.loc[0:, 'eventtime'])
    # pandas.to_numeric(FACATable.loc[0:, 'id'])
    print('数据处理完成')
    print('------------')
    return [FACATable, traceMissingTable, sheetData]


def insertIntoSql(frames, tableNames):
    # 数据插入PG sql
    db_url = 'mssql+pymssql://PBIuser:PBIUser123456@CNWXIM0TRSQLV4A/Metal_Robot'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    for i in range(len(frames)):
        if frames[i] is None:
            continue
        frames[i].to_sql(tableNames[i], engine, index=False, if_exists='append', method='multi', index_label=None,
                 chunksize=None,)
    engine.dispose()
    print('数据写入SQL Server成功')
    print('------------')


def getMailData(project):
    # pandas.DataFrame(dataFrame).to_excel('C:\ActionTracker\data1.xlsx', sheet_name='summery', index=False,
    # header=True)
    tableColumns = ['日期', '被卡控站点', '呈现站点', '漏失数量', '漏失率', 'FA', 'CA', 'DRI']
    sqlData = f""" SELECT DISTINCT top (30) 
                         A.日期, 被卡控站点, 呈现站点, 漏失数量, 漏失率, FA, CA, DRI
                         FROM actiontracker_tracemissing A INNER JOIN actiontracker_faca B
                         ON A.item_id = B.item_id
                         WHERE ((A.日期 = '{dateNow}' and b.eventtime = '{dateNow}') 
                         or (A.returntime='1900-01-01 00:00:00.000' or A.returntime is null ))
                         and A.item_id like '{project}%' and B.item_id like '{project}%'
                         ORDER BY A.日期 DESC """  # 选择当天数据和之前未回复的数据,按日期倒序排列
    mailInfoTable = pandas.DataFrame(conSqlS(sqlData))  # 从数据库获取需回复FACA信息
    mailInfoTable.columns = tableColumns
    mailInfoTable['日期'] = mailInfoTable['日期'].astype(str)  # 强制转换数据类型
    for i in range(len(mailInfoTable.loc[0:, '漏失率'])):
        mailInfoTable.loc[i, '漏失率'] = '{:.2%}'.format(float(mailInfoTable.loc[i, '漏失率']))  # 格式化输出%数据
        # mailInfoTable.loc[i, '日期'] = mailInfoTable.loc[i, '日期'].strftime('%Y-%m-%d')[0:10]
    print('------------')
    styles = [dict(selector='td', props=[("text-align", "center")]),
              dict(selector="th", props=[("font-size", "15px"), ("text-align", "center")]),
              dict(selector="caption", props=[("caption-side", "top")])]
    mailInfoTable = mailInfoTable.style.set_table_styles(styles).applymap(changeColor,
                                                                          subset=['漏失率']).hide_index().render()
    mailInfoTable = changeStyle(mailInfoTable, dateNow)
    return mailInfoTable


def conSqlS(sqlData):
    # try:
    conn = pymssql.connect(
        host='CNWXIM0TRSQLV4A',  # 主机名或ip地址
        user='PBIuser',  # 用户名
        password='PBIUser123456',  # 密码
        charset='UTF-8',  # 字符编码
        database='Metal_Robot')  # 库名
    cursor = conn.cursor()
    cursor.execute(sqlData)
    row = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()
    print('数据库操作完成~')
    return row


def changeStyle(mailInform, dateNow1):
    # 未达标邮件~
    with open(imagePath, "rb") as f:  # 图片转为base64格式
        base64_data = base64.b64encode(f.read())
        imageBase64 = base64_data.decode()
        # print(imageBase64)

    head = \
        """
        <head>
            <meta charset="utf-8">
            <STYLE TYPE="text/css" MEDIA=screen>
                table
            {border-collapse:collapse;}
            table, td,tr,th
            {border:1px solid black;padding: 15px}
            </STYLE>
        </head>
        """
    body = \
        """
       <body>
        <div align="left",font-family='SimSun',"font-size"="15px">
           <p>Dear Sirs:</p>
           <br>
           <p>以下为{project}专案Trace漏失报告：</p>
           <p>漏失发生日期：{sh_sys}</p>
           <p><img src="data:image/png;base64,{imageBase64}" alt=" " width="1400"></p> 
           <br>
           <p>以下为Trace漏失FACA,请相关DRI尽快回复！</p>
           <p>{dataFrame}</p>
           <br>
           <a href="https://apps.powerapps.com/play/220d9b3e-6604-4257-9eb3-79e06c05f5b2?tenantId=bc876b21-f134-4c12-
           a265-8ed26b7f0f3b&source=portal&screenColor=rgba(0%2C%20176%2C%20240%2C%201)
           "target="_blank">请用手机登录Power APPs，进入应用:Trace漏失，回复FA/CA！</a>
           <br>
           <br>
           <br> 
           <a href="http://cnwgpm0pbi01/Reports/powerbi/WX_MetalSME/official/TEST/Trace%E7%94%9F%E4%BA%A7%E6%BC%8F%E5%A4%B1"
           target="_blank">更多Trace漏失数据请查阅PowerBi，请用Jabil网络登录！</a>
           <br>
           <br>
           <br>
           <p>有任何问题或建议，请联系我们~</p>
           <br>
           <br>
           <p>Best Regards!</p>
           <p>Send by: OE-FoF</p>
           <p>Tel: 8526-6258</p>
           <p>自动邮件,请勿答复</p>
        </div>
    </body>
    """.format(dataFrame=mailInform, project=project, sh_sys=str(dateNow1)[0:10], imageBase64=imageBase64)
    mailText = "<html>" + head + body + "</html>"
    # dataFrame = dataFrame.replace('&&&', '<br>')
    print('mail格式处理完毕')
    print('------------')
    return mailText


def changeStyle2(dateNow1):
    # 达标邮件~
    head = \
        """
        <head>
            <meta charset="utf-8">
            <STYLE TYPE="text/css" MEDIA=screen>
                table
            {border-collapse:collapse;}
            table, td,tr,th
            {border:1px solid black;padding: 15px}
            </STYLE>
        </head>
        """
    body = \
        """
       <body>
        <div align="left",font-family='SimSun',"font-size"="15px">
           <p>Dear Sirs:</p>
           <br>
           <p>以下为{project}专案Trace漏失报告：</p>
           <p>漏失发生日期：{sh_sys}</p>
           <br>
           <p>今日{project}专案各站漏失率均达标，谢谢各位的努力~</p>
           <br>
           <br>
           <br> 
           <br>
           <br>
           <br> 
           <a href="https://apps.powerapps.com/play/220d9b3e-6604-4257-9eb3-79e06c05f5b2?tenantId=bc876b21-f134-4c12-
           a265-8ed26b7f0f3b&source=portal&screenColor=rgba(0%2C%20176%2C%20240%2C%201)
           "target="_blank">请用手机登录Power APPs，进入应用:Trace漏失，回复FA/CA！</a>
           <br>
           <br>
           <br> 
           <a href="http://cnwgpm0pbi01/Reports/powerbi/WX_MetalSME/official/TEST/Trace%E7%94%9F%E4%BA%A7%E6%BC%8F%E5%A4%B1"
           target="_blank">更多Trace漏失数据请查阅PowerBi，请用Jabil网络登录！</a>
           <br>
           <br>
           <br>
           <p>有任何问题或建议，请联系我们~</p>
           <br>
           <br>
           <p>Best Regards!</p>
           <p>Send by: OE-FoF</p>
           <p>Tel: 8526-6258</p>
           <p>自动邮件,请勿答复</p>
        </div>
    </body>
    """.format(project=project, sh_sys=str(dateNow1)[0:10])
    mailText = "<html>" + head + body + "</html>"
    print('mail格式处理完毕')
    print('------------')
    return mailText


def changeColor(val):
    """
    :param val:元素值，表达式datafarmede中的所有值，单个传入 对单个值的字体颜色修改
    :return: val
    """
    try:
        if float(val[:-1]) > 0.05:  # 报警规则
            return 'color:red'
        else:
            return 'color:black'
    except Exception as e:
        print(e)
        return 'color:black'


def conPlotData(date):  # 链接数据库，查询已处理的时段
    querySql = f"""select top (15)
                        日期,
                        sum(总数) as 总数,
                        avg(漏失率) as 漏失率,
                        sum(漏失数量) as 漏失数量
                    from actiontracker_data
                    where project = '{project}'
                    group by 日期
                    order by 日期 desc
                    """  # 趋势图
    querySql2 = f"""select top (5) 站别,
                           sum(漏失数量) as 漏失数量
                    from actiontracker_data
                    where project = '{project}'
                    group by 站别
                    order by 漏失数量 desc
                    """  # 月度柏拉图
    querySql3 = f"""select top (5) 站别,
                            sum(漏失数量) as 漏失数量,
                            LEFT(CONVERT(NVARCHAR(40),DATEADD(day , -1, GETDATE()),120),10)+' 00:00:00' as 日期
                        from actiontracker_data
                        where 日期='{date}'
                        and project = '{project}'
                        group by 站别
                        order by 漏失数量 desc
                    """  # 天柏拉图
    db_url = 'mssql+pymssql://PBIuser:PBIUser123456@CNWXIM0TRSQLV4A/Metal_Robot'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute(querySql)
    row = cursor.fetchall()
    # row =  [''.join(i) for i in row]    #元组转化为列表
    df = pandas.DataFrame(row)
    df.columns = [
        '日期',
        '总数',
        '漏失率',
        '漏失数量']
    cursor.execute(querySql2)
    row = cursor.fetchall()
    df1 = pandas.DataFrame(row)
    df1.columns = [
        '站别',
        '漏失数量']
    cursor.execute(querySql3)
    row = cursor.fetchall()
    df2 = pandas.DataFrame(row)
    df2.columns = [
        '站别',
        '漏失数量',
        '日期']
    connection.commit()
    cursor.close()
    print('------------')
    print("数据获取完毕~")
    return [df, df1, df2]


def createPlot(date, imagePath1):
    dfs = conPlotData(date)
    # matplotlib.rc("font", family='YouYuan', weight="bold")  # 设置字体,显示中文
    matplotlib.rc("font", family='YouYuan', weight="bold")  # 设置字体,显示中文
    plt.rcParams["font.family"] = 'YouYuan'
    plt.rcParams['font.sans-serif'] = ['YouYuan']  # 中文乱码
    plt.rcParams['axes.unicode_minus'] = False  # 正负号
    backgroundColor = 'white'
    barColor = '#63A6E2'
    lineColor = '#E17932'
    titleColor = 'black'
    df, df1, df2 = dfs[0], dfs[1], dfs[2]
    x = df.iloc[0:, 0]
    y1 = df.iloc[0:, 1]
    y2 = df.iloc[0:, 2] * 100
    # fig, axs = plt.subplots(constrained_layout=False, figsize=(20.16, 14.4),
    #                        dpi=80)  # 设置画布  # 像素（1080， 720）= figsize*dpi
    fig = plt.figure(figsize=(20, 15), dpi=300, facecolor=backgroundColor,
                     edgecolor='red')  # 设置画布  # 像素（1080， 720）= figsize*dpi
    ax1 = fig.add_subplot(212)  # 设置图标位置
    rect = ax1.patch  # 设置背景色
    rect.set_facecolor(backgroundColor)
    ax1.bar(x, y1, color=barColor, width=0.6, bottom=0, align='center')  # 柱形图
    ax1.set_ylabel('Total Number(million)', fontsize=16, color=titleColor)
    ax2 = ax1.twinx()
    ax2.plot(x, y2, color=lineColor, linewidth=2)  # 折线图
    ax2.set_ylabel('Leak Rate(%)', fontsize=16, color=titleColor)  # 设置标签
    ax2.axhline(y=0.1, ls=":", c="red")  # 添加水平直线
    ax1.xaxis.set_major_locator(ticker.MultipleLocator(1))  # 设置刻度间隔
    for tick in ax1.get_xticklabels():  # 刻度标签旋转
        tick.set_rotation(15)
    for a, b in zip(x, y1):  # 设置数字标签**
        ax1.text(a, b, '{:,}'.format(b), ha='center', va='bottom', fontsize=13)  # a, b 为标签位置       '%.0f' % b
    for a, b in zip(x, y2):  # 设置数字标签**
        ax2.text(a, b, '{:.3%}'.format(b / 100), ha='center', va='bottom', fontsize=11)  # a, b 为标签位置       '%.0f' % b

    ax3 = fig.add_subplot(221)
    rect = ax3.patch  # 设置背景色
    rect.set_facecolor(backgroundColor)
    total = df1.agg('sum')
    total = pd.to_numeric(total.iloc[1])
    x = df1.iloc[0:, 0]
    y = df1.iloc[0:, 1]
    rat = df1.iloc[0:, 1] / total
    y2 = []
    j = 0
    for i in range(len(rat)):
        j += rat.iloc[i]
        y2.append(j)
    ax3.bar(x, y, color=barColor, width=0.6, bottom=0, align='center')  # 柱形图
    ax3.set_ylabel('Leak Number(Last 15 Days)', fontsize=16, color=titleColor)
    ax4 = ax3.twinx()
    ax4.plot(x, y2, color=lineColor, linewidth=2)  # 折线图
    ax4.set_ylim([0, 1.1])
    ax4.set_yticks([])  # 设置轴标签为空
    for tick in ax3.get_xticklabels():  # 刻度标签旋转
        tick.set_rotation(7.5)
    xticks = ax3.get_xticks()  # 获取x标签的坐标值
    for a, b in zip(xticks, y):  # 设置数字标签**
        ax3.text(a, b, '%.0f' % b, ha='center', va='bottom', fontsize=13)  # a, b 为标签位置
    for a, b in zip(xticks, y2):  # 设置数字标签**
        ax4.text(a, b, '{:.2%}'.format(b), ha='center', va='bottom', fontsize=11)  # a, b 为标签位置

    ax5 = fig.add_subplot(222)
    rect = ax5.patch  # 设置背景色
    rect.set_facecolor(backgroundColor)
    total = df2.agg('sum')
    total = pd.to_numeric(total.iloc[1])
    x = df2.iloc[0:, 0]
    y = df2.iloc[0:, 1]
    rat = df2.iloc[0:, 1] / total
    y2 = []
    j = 0
    for i in range(len(rat)):
        j += rat.iloc[i]
        y2.append(j)
    ax5.bar(x, y, color=barColor, width=0.6, bottom=0, align='center')  # 柱形图
    ax5.set_ylabel('Leak Number(Last Day)', fontsize=16, color=titleColor)
    ax6 = ax5.twinx()
    ax6.plot(x, y2, color=lineColor, linewidth=2)  # 折线图
    ax6.set_ylim([0, 1.1])
    ax6.set_yticks([])
    for tick in ax5.get_xticklabels():  # 刻度标签旋转
        tick.set_rotation(7.5)
    xticks = ax5.get_xticks()  # 获取x标签的坐标值
    for a, b in zip(xticks, y):  # 设置数字标签**
        ax5.text(a, b, '%.0f' % b, ha='center', va='bottom', fontsize=13)  # a, b 为标签位置
    for a, b in zip(xticks, y2):  # 设置数字标签**
        ax6.text(a, b, '{:.2%}'.format(b), ha='center', va='bottom', fontsize=11)  # a, b 为标签位置

    plt.savefig(imagePath1, bbox_inches='tight')
    print("图片保存完毕~")
    return


class SendMail(object):
    def __init__(self, content=None,
                 image=None, file=None):
        """
               :param recv: 收件人，多个要传list ['a@qq.com','b@qq.com]
               :param content: 邮件正文
               :param image: 图片路径，绝对路径，默认为无图片
               :param file: 附件路径，如果不在当前目录下，要写绝对路径，默认没有附件
        """

        self.recv = people_email  # 收件人，多个要传list ['a @ qq.com','b @ qq.com]
        self.cc_email = cc_email  # cc人员
        self.title = f'{project}专案Trace漏失报告'  # 邮件标题
        self.content = content  # 邮件正文
        self.image = image  # 图片路径（绝对路径）
        self.file = file  # 文件路径（绝对路径）
        self.message = MIMEMultipart()  # 构造一个MIMEMultipart对象代表邮件本身

        # 添加文件到附件
        if self.file:
            file_name = os.path.split(self.file)[-1]  # 只取文件名，不取路径
            try:
                f = open(self.file, 'rb').read()
            except Exception as e:
                traceback.print_exc()
            else:
                att = MIMEText(f, "base64", "utf-8")
                att["Content-Type"] = 'application/octet-stream'
                # base64.b64encode(file_name.encode()).decode()
                new_file_name = '=?utf-8?b?' + base64.b64encode(file_name.encode()).decode() + '?='
                # 处理文件名为中文名的文件
                att["Content-Disposition"] = 'attachment; filename="%s"' % (new_file_name)
                self.message.attach(att)

        # 添加图片到附件
        if self.image:
            try:
                with open(self.image, 'rb') as f:
                    # 将图片显示在邮件正文中
                    msgimage = MIMEImage(f.read())
                    msgimage.add_header('Content-ID', '<image1>')  # 指定文件的Content-ID,<img>,在HTML中图片src将用到
                    self.message.attach(msgimage)
            except Exception as e:
                traceback.print_exc()

    def send_mail(self):
        mailTitle = self.title
        from_addr = "OE-FoF_SupportTeam@jabil.com"
        receivers = self.recv
        cc = self.cc_email
        to_addrs = receivers + cc
        smtpServer = r'CORIMC04'
        commonPort = 587
        smtp = smtplib.SMTP("{}:{}".format(smtpServer, commonPort))
        self.message.attach(MIMEText(self.content, 'html', 'utf-8'))  # 正文内容   plain代表纯文本,html代表支持html文本
        self.message['From'] = from_addr  # 发件人
        self.message['To'] = ','.join(receivers)  # 收件人
        self.message['Subject'] = Header(mailTitle, 'utf-8').encode()  # 邮件标题
        self.message['Cc'] = ','.join(cc)  # cc
        # 发送邮件服务器的对象
        print('收件人为------------------')
        print(receivers)
        print('抄送------------------')
        print(cc)
        try:
            smtp.sendmail(from_addr=from_addr, to_addrs=to_addrs, msg=self.message.as_string())
            print("邮件发送成功！")
            pass
        except Exception as e:
            resultCode = 0
            traceback.print_exc()
        else:
            resultCode = 1
        smtp.quit()
        return resultCode


if __name__ == '__main__':
    iniPath = os.path.abspath(os.path.dirname(sys.argv[0]))  # 获取当前执行文件夹路径
    iniPath = [iniPath + "/actionTracker.ini", iniPath + "/actionTracker2.ini", iniPath + "/actionTracker3.ini"]
    for path0 in iniPath:
        try:
            config = configparser.ConfigParser()
            try:
                config.read(path0, encoding="utf-8-sig")  # 导出配置文件,解决文档格式为utf-8-bom问题
            except Exception as e:
                print(e)
                config.read(path0, encoding="utf-8")  # 导出配置文件
            people_email = config.get("messages", "people_email")  # 接收邮件人员
            cc_email = config.get("messages", "cc_email")  # cc邮件人员
            people_email = people_email.split(",")  # 字符串根据","进行分割，转换为list
            cc_email = cc_email.split(",")
            sourcePath = config.get("messages", "sourcePath")  # 数据源地址
            # outputPath = config.get("messages", "outputPath")  # 处理后文件路径
            dayAdd = int(config.get("messages", "dayAdd"))  # 取之前多少天的日期
            reMail = config.get("messages", "remail")  # 判断是否允许重复发邮件， 1为允许
            imagePath = config.get("messages", "imagePath")  # 图片路径
            project = sourcePath.split("ctionTracker")[1].split(" ")[0][1:]  # 专案
            SQLTableName = ['actiontracker_faca', 'actiontracker_tracemissing', 'actiontracker_data']  # 数据库表名称

            date0 = datetime.datetime.strptime("1900-01-01", "%Y-%m-%d")
            dateNow = datetime.datetime.strptime(
                (datetime.datetime.now() + datetime.timedelta(days=dayAdd)).strftime("%Y-%m-%d"),
                "%Y-%m-%d")
            dateDays = (dateNow - date0).days + 2
            print('==================================')
            print(f'专案为：{project} 获取的数据日期为：{dateNow}, 数字格式为：{dateDays}')
            sql = f"SELECT DISTINCT 日期 FROM actiontracker_data WHERE project='{project}'"
            dates = conSqlS(sql)
            dates = [i[0].strftime("%Y-%m-%d") + ' 00:00:00' for i in dates]
            isRun = (f'{dateNow}' in dates)  # 判断今日数据是否已运行，True为已运行数据， False为未运行
            dataIsNone = False  # 判断今日漏失数据是否为空，True为空，今日无漏失数据， False不为空，今日有楼市数据
            mailTime = datetime.datetime.now().strftime("%Y-%m-%d")
            mailTime = mailTime[0:10]

            try:
                dataList = readDataSource(sourcePath)  # 1.从excel读取数据源
            except Exception as e:
                print(e)
                print("当日数据未更新，请更新数据~")
                continue
            dataList = dataSourceSplit(dataList)  # 2.对数据进行分析，拆成两个表
            print('TraceMissing数据库数据为-----')
            print(dataList[1])

            if (not isRun) and (not dataIsNone):  # 数据没有运行，且数据不为空时，存入数据库。防止重复存入数据库
                print('当日漏失数据存入数据库中、、、')
                insertIntoSql(dataList, SQLTableName)  # 3.数据存入PGSQL
            elif (not isRun) and dataIsNone:  # 数据没有运行，且数据为空时，存入数据库，发送无漏失邮件。
                print('当日无漏失数据~')
                if datetime.datetime.now().hour != 8:  # 防止重复运行重复存入数据库重复发邮件
                    print('当天邮件已发送~')
                    continue
                insertIntoSql(dataList, SQLTableName)  # 3.数据存入PGSQL
                mailInfoTable = changeStyle2(dateNow)
                m = SendMail(content=mailInfoTable)
                m.send_mail()  # 7. 发Trace无漏失邮件
                continue

            createPlot(dateNow, imagePath)  # 从数据库生成图片
            mailInfo = getMailData(project)  # 6. 从数据库获取邮件信息pd
            # openExcel(outputPath)  # 模拟电脑打开excel
            if isRun & (reMail != '1'):  # 防止重复运行重复存入数据库重复发邮件
                print('当天邮件已发送~')
                continue
            m = SendMail(content=mailInfo)
            m.send_mail()  # 7. 发Trace漏失邮件
        except Exception as e:
            print(e)
            continue
    sys.exit(0)
