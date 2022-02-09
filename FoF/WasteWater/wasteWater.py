#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: DagTraceKafka.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site:
# @Time: 12月 20, 2021
# ---


import configparser
import datetime
import io
import os
import smtplib
import sys
from email.header import Header
from email.mime.text import MIMEText
import pandas
import requests
import simplejson
import sqlalchemy
import sqlalchemy.sql.default_comparator
from numpy import array as array
from numpy import mat as mat
from numpy import unique as unique


# 找到公司内部所有的MN，传给下个参数,配置文件的(keys)指密钥,因为经常改动,所有写在配置文件上
def find_mn(keys):
    """
    :type str
    :param keys:密钥
    :return:json
    """
    # url = "http://140.179.43.204:7065/BaseService/MN_BaseExt/GetMN_BaseList?UserInfoID=" + keys
    url = "http://140.179.72.45:7065/BaseService/MN_BaseExt/GetMN_BaseList?UserInfoID=" + keys
    payload = {}
    headers = {
        'Authorization': 'LDKJ=5771891D20087C4A111BADC7F9FD9642',
        'Content-Type': 'application/json'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    MN_url = simplejson.loads(response.text)
    MN_url = pandas.DataFrame.from_records(MN_url)
    MN = MN_url['MN']
    MN = unique(MN)
    return MN


# 定义是否超标的函数，给数据计算迭代用的
def function(a, b):
    """
    :param a:数字，datafarme的某列
    :param b: 数字，datafarme的某列
    :return:now
    """
    if a <= b:
        return 'NO'
    else:
        return 'YES'


# 查询此小时对应的MN下的数据 清理并计算 因为此数据可能为空值为了不计算报错
# 所有此处用try excep 方法，不为空计算，为空的全部给出MN和time，后面全部给0
# mn是上面find_mn出来的mn，starttime，endtime分别为时间的开始时间和结束时间，按1小时的话两者是相等的
# 但如果是多个小时两者会不相同，且都为整点
def waste_history(mn, starttime, endtime):
    """
    :type mn str
    :type starttime datatime
    :param mn:MN
    :param starttime: start time
    :param endtime:end time
    :return:  response_array
    """
    try:
        url1 = "http://140.179.72.45:7065//BusinessService/MN_HisData/GetAllMN_HisData_MN?MN="
        # url1 = 'http://140.179.43.204:7065//BusinessService/MN_HisData/GetAllMN_HisData_MN?MN='
        url = url1 + str(mn) + '&' + 'StartTime=' + str(starttime) + '&' + 'EndTime=' + str(
            endtime) + '&' + 'DataTypeID=2061' + '&' + '&' + 'Rubbish=false' + '&' + 'ValueTypeID=Avg'
        # print(url)
        payload = {}
        files = {}
        headers = {
            'Authorization': 'LDKJ=5771891D20087C4A111BADC7F9FD9642',
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload, files=files)
        response = simplejson.loads(response.text)

        response_array = pandas.DataFrame.from_records(response)

        response_array = pandas.DataFrame(response_array, columns=['DataTime', 'DataTypeID', 'DataValue',
                                                                   'LHCodeID', 'ValueTypeID', 'BZlevelDown',
                                                                   'BZlevelUP', 'MN', 'MNName', 'ParamName',
                                                                   'ParamUnit'])
        response_array['DataTime'] = response_array['DataTime'].astype(str)
        response_array['DataTime'] = response_array['DataTime'].str[6:16]
        response_array['DataTime'] = response_array['DataTime'].astype('int')
        response_array['DataTime'] = pandas.to_datetime(response_array['DataTime'], unit='s')
        response_array['DataTime'] = response_array['DataTime'] + pandas.DateOffset(hours=8)
        pandas.set_option('display.max_columns', None)
        pandas.set_option('display.max_rows', None)
        response_array['BZlevelUP'] = response_array['BZlevelUP'].fillna(0)
        response_array['result'] = response_array.apply(lambda x: function(x.DataValue, x.BZlevelUP), axis=1)
        response_array = mat(response_array)
        response_array = array(response_array)
        # print("链接接口成功")
    except Exception as e:
        response_array = pandas.DataFrame(
            columns=['DataTime', 'DataTypeID', 'DataValue', 'LHCodeID', 'ValueTypeID', 'BZlevelDown', 'BZlevelUP', 'MN',
                     'MNName', 'ParamName', 'ParamUnit'])
        response_array.loc[0, 'DataTime'] = starttime
        response_array.loc[0, 'MN'] = str(mn)
        response_array = response_array.fillna(0)
        response_array = mat(response_array)
        response_array = array(response_array)
        print("无法链接接口,报错信息为：" + str(e))
    return response_array


# 更改字体颜色的方法，给下面更改样式的使用,pandas.style.applymap会直接传参,val代表每一个datafarme里面的元素.


# 更改表格样式，fort指全部导出的数据，也可以在主程序下进行筛选,输出的html_masg作为html格式,给sent_mail作参.
def change_style(fort_can):
    fort = pandas.DataFrame(fort_can, columns=['ParamName', 'MNName', 'DataValue', 'BZlevelUP'])
    fort = fort.rename(columns={'ParamName': '项目', 'MNName': '地点', 'DataValue': '值', 'BZlevelUP': '上限'})
    fort = fort.reset_index(drop=True)
    fort.index = fort.index + 1
    fort['值'] = fort['值'].map(lambda x: '%.2f' % x)
    fort['上限'] = fort['上限'].map(lambda x: '%.2f' % x)
    styles = [
        dict(selector="th", props=[("bgcolor", "#BEBEBE"),
                                   ("text-align", "center")]),

    ]

    fort = fort.style. \
        set_table_styles(styles). \
        applymap(change_color1, subset=['值']). \
        hide_index().render()

    head = \
        """
        <head>
            <meta charset="utf-8">
            <STYLE TYPE="text/css" MEDIA=screen>
            table
            {
             border-collapse:collapse;

            }
            table, td,tr,th
            {
             border:1px solid black;

             padding: 15px             
             }
            </STYLE>
        </head>
        """
    body = \
        """
        <body >
            <div align="left",font-family='SimSun',"font-size"="15px">   
            <p>Dear Leaders:</p>
            <p>存在废水数据超标,请注意！</p>
            <p>日期：{datetime_html} 时间：{starttime}—{endtime}</p>
            <p>{fort}</p>
            <p>现场管理:徐虎 18661001021 周思翰 18961885579<p>
            <br/>
            <a href="http://cnwgpm0pbi01/Reports/powerbi/WX_MetalSME/official/JGP%E5%BA%9F%E6%B0%B4%E7%9B%91%E6%B5%8B%E6%8A%A5%E5%91%8A"target="_blank">更多废水数据请查阅PowerBi</a>
            <p>自动邮件,请勿答复</p>
            </div>
    </body>
    """.format(fort=fort, starttime=str(starttime)[11:16], endtime=str(last_time)[11:16],
               datetime_html=str(starttime)[0:10])
    # 修改html,制作email样式模板.
    html_msg = "<html>" + head + body + "</html>"

    return html_msg
    # 调整发送mail时候的数据样式，确立邮件上的时间
    # 转为html,发送到mail中去


def change_color(val):
    # color = 'red' if 1 < 2 else 'black'
    return 'color:black'


def change_color1(val):
    return 'color:red'


def change_style_df(df):
    """
    没在用了
    :param df:
    :return:
    """
    dataFrame = pandas.DataFrame(df, columns=['DataTime', 'ParamName', 'MNName', 'DataValue', 'BZlevelUP', 'result'])
    dataFrame = dataFrame.rename(
        columns={'DataTime': '时间', 'ParamName': '项目', 'MNName': '地点', 'DataValue': '值', 'BZlevelUP': '上限',
                 'result': '结果'})
    dataFrame = dataFrame.reset_index(drop=True)
    dataFrame.index = dataFrame.index + 1
    dataFrame = dataFrame.sort_values(by=['结果', '地点', '时间'], ascending=[False, True, True])
    # dataFrame = pd.to_numeric(dataFrame.iloc[3])
    # dataFrame = pd.to_numeric(dataFrame.iloc[4])
    dataFrame["值"] = dataFrame["值"].astype("float")
    dataFrame["上限"] = dataFrame["上限"].astype("float")
    # print(dataFrame)
    try:
        a = dataFrame[(dataFrame['结果'] == 'YES')].index.tolist()
    except:
        print("无报错信息~")
        a = dataFrame[(dataFrame['结果'] == 'NO')].index.tolist()
    styles = [dict(selector='td', props=[('border', '0.1px solid black'),
                                         ('padding', '25px'),
                                         ("table rules", "none"),
                                         ("cellspacing", "1"),
                                         ("border-collapse", "collapse"),
                                         ("text-align", "center")]),
              dict(selector="th", props=[("font-size", "100%"),
                                         ("border-collapse", "collapse"),
                                         ("table rules", "rows"),
                                         ('border', '0.1px solid black'),
                                         ('padding', '25px'),
                                         ("text-align", "center")]),
              dict(selector="caption", props=[("caption-side", "top")])
              ]  # 字典的方式给下面的style传值
    # print("报错信息：")
    if len(a) != 0:
        dataFrame = dataFrame.sort_values(by=['地点', '时间'], ascending=[True, True])
        dataFrame = dataFrame.style. \
            set_table_styles(styles). \
            applymap(change_color,
                     subset=pandas.IndexSlice[float(a[0]):float(a[len(a) - 1]), ['时间', '项目', '地点', '值', '上限', '结果']]). \
            hide_columns(['结果']). \
            format({'值': '{:.1f}', '上限': '{:.1f}'}). \
            hide_index().render()

    else:
        # print('2')
        # print(dataFrame)
        dataFrame = dataFrame.sort_values(by=['地点', '时间'], ascending=[True, True])
        dataFrame = dataFrame.style. \
            set_table_styles(styles). \
            hide_columns(['结果']). \
            hide_index().render()
    # print('1')
    head = \
        """
        <head>
            <meta charset="utf-8">
            <STYLE TYPE="text/css" MEDIA=screen>
                body {
                }

                h1 {
                    color: #000000
                }

                div.header h2 {
                    color: #000000;
                    font-family: 宋体;
                }

                h3 {
                    font-family: 宋体;
                    font-size: 20px;
                    margin: auto;
                    text-align: left;     
                }
                h4 {
                    font-family: 宋体;
                    font-size: 20px;
                    margin: auto;
                    text-align: left;
                }
                h5{
                    color: #000000;
                    font-family: 宋体;
                    font-size: 18px;
                    margin: auto;
                    text-align: left;
                }
            </STYLE>
        </head>
        """

    body = \
        """
        <body>
        <div align="center" class="header">
            <h1 align="center">JGP无锡废水数据详情</h1>
        </div>

        <div class="content">
            <!--正文内容-->
            <h2>Dear leader:</h2>
            <h3>以下为{starttime_html}-{endtime_html}废水数据详情，请查阅！</h3>
            <a href="http://cnwgpm0pbi01/Reports/powerbi/WX_MetalSME/official/JGP%E5%BA%9F%E6%B0%B4%E7%9B%91%E6%B5%8B%E6%8A%A5%E5%91%8A"target="_blank">进入PowerBi查看详情</a>         
        <div>
            <h4>{dataFrame}</h4>
        </div>

        <p style="text-align: center">
            —— 本次报告完 ——
        </p>
    </div>
    </body>
    """.format(dataFrame=dataFrame, starttime_html=endtime_oftotal, endtime_html=starttime)
    # 修改html,制作email样式模板.

    dataFrame = "<html>" + head + body + "</html>"
    # print(dataFrame)

    return dataFrame


# 插入数据库，pagedata为fort,必须是全部导出的数据，无需筛选.
def insert_intosql(pagedata):
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    output = io.StringIO()
    pagedata.to_csv(output, sep='\t', index=False, header=False)
    output.seek(0)
    cursor.copy_from(output, 'wastewater', null='')
    connection.commit()
    cursor.close()
    engine.dispose()
    print('sql插入完成')


def sentEmailOutlook(waring_html, time_email, people_email):
    mail_content = ''
    if time_email == starttime:
        mail_content = "JGP无锡废水超标报警"
    elif time_email == excel_time2:
        mail_content = 'JGP无锡污废水实验室报告'
    else:
        mail_content = 'JGP无锡污废水实验室报告'
    smtpServer = r'CORIMC04'
    commonPort = 587
    smtp = smtplib.SMTP("{}:{}".format(smtpServer, commonPort))
    msg = MIMEText(waring_html, "html", 'utf-8')
    msg["Subject"] = Header(mail_content, 'utf-8').encode()
    from_addr = "Facility_Service@jabil.com"
    receivers = people_email
    cc = cc_email
    to_addrs = receivers + cc
    cc = list(cc)
    # receivers=','.join(receivers)
    receivers = list(receivers)
    msg["To"] = ",".join(receivers)
    msg["from"] = from_addr
    msg['Cc'] = ",".join(cc)
    smtp.sendmail(from_addr=from_addr, to_addrs=to_addrs, msg=msg.as_string())
    smtp.quit()
    print("邮件发送成功~")


if __name__ == '__main__':
    while True:  # 根据开始时间循环运行到现在时间
        try:
            config = configparser.ConfigParser()
            path0 = os.path.abspath(os.path.dirname(sys.argv[0]))  # 获取当前执行文件夹路径
            path0 = path0 + "/water.ini"
            # print(path0)
            try:
                config.read(path0, encoding="utf-8-sig")  # 导出配置文件,解决文档格式为utf-8-bom问题
            except Exception as e:
                print(e)
                config.read(path0, encoding="utf-8")  # 导出配置文件
            starttime = config.get("messages", "starttime")
            print("运行数据时间为：" + starttime)
            endtime = config.get("messages", "endtime")  # 设定导出数据的时间,这个一般是当前小时的前一个小时，可修改为多个小时
            keys = config.get("messages", "keys")  # 密钥
            excel_time2 = config.get('messages', 'excel_time_2')
            cc_email = config.get("messages", "cc_email")
            people_email = config.get("messages", "people_email")
            people_email = people_email.split(",")
            cc_email = cc_email.split(",")
            starttime = datetime.datetime.strptime(str(starttime), '%Y-%m-%d %H:%M:%S')
            endtime = datetime.datetime.strptime(str(endtime), '%Y-%m-%d %H:%M:%S')  # 修改时间格式,传入find_mn中
            endtime_oftotal = starttime + datetime.timedelta(hours=-12)
            last_time = starttime + datetime.timedelta(hours=+1)
            if starttime > datetime.datetime.now() + datetime.timedelta(hours=-1):
                break
            if last_time > datetime.datetime.now():
                last_time = datetime.datetime.now().replace(minute=0, second=0, microsecond=0) + datetime.timedelta(
                    hours=-1)
            totalTime = config.get("messages", "totalTime")
            # totalTime 确定需要定时发送的时间，此确定时间写在配置文件上,对比的时间为starttime.一旦相同则导出12个小时的数据.

            fort = []
            MN = list(find_mn(keys))  # 导出mn
            # print(MN)
            for o in MN:
                fort.extend(waste_history(o, starttime, endtime))  # 遍历mn,导出mn相关数据
            fort = pandas.DataFrame.from_records(fort,
                                                 columns=['DataTime', 'DataTypeID', 'DataValue', 'LHCodeID',
                                                          'ValueTypeID',
                                                          'BZlevelDown', 'BZlevelUP', 'MN', 'MNName', 'ParamName',
                                                          'ParamUnit', 'result'])
            print("修改报警参数上限")
            for i in range(len(fort)):  # 根据项目修改上限
                if fort.iloc[i, 9] == 'CODcr':
                    fort.iloc[i, 6] = 500
                elif fort.iloc[i, 9] == 'pH':
                    fort.iloc[i, 6] = 9
                elif fort.iloc[i, 9] == '氨氮':
                    fort.iloc[i, 6] = 45
                elif fort.iloc[i, 9] == '总铬':
                    fort.iloc[i, 6] = 0.5
                elif fort.iloc[i, 9] == '总磷':
                    fort.iloc[i, 6] = 8
                elif fort.iloc[i, 9] == '总镍':
                    fort.iloc[i, 6] = 0.1
                elif fort.iloc[i, 9] == '总氮':
                    fort.iloc[i, 6] = 70
                if float(fort.iloc[i, 2]) < float(fort.iloc[i, 6]):  # 判断是否报警
                    fort.iloc[i, 11] = 'NO'
                    # print("修改为NO")
                elif fort.iloc[i, 9] == '水流量':
                    fort.iloc[i, 11] = 'NO'

            insert_intosql(fort)  # 导出的全部数据插入sql
            print('数据插入成功')
            # # 后续需要判断发送半天时间，和判断发送报警邮件。
            # #如果有超标且定点发送群体邮件的的是需要改变style.color,一个小时内的超标预警不需要,可以做两个style给出.因为会有两种样式.
            # #后续会来两天发送一个的那种邮件.再做第三个style
            # print(fort)
            # fort.to_csv('wasteWater.txt')
            fort_yes = fort[
                (fort['result'] == 'YES') & (fort['BZlevelUP'] != float(0)) & (fort['ValueTypeID'] == 'Avg')]  #
            ll = list(fort_yes['result'])
            # print(ll)

            # 判断是否存在超标的数据,超出的发送预警
            if 'YES' in ll:
                pandas.set_option('display.max_columns', None)
                pandas.set_option('display.max_rows', None)
                html_waring = change_style(fort_yes)
                print("发送报错邮件-----========")
                # print(html_waring)
                sentEmailOutlook(waring_html=html_waring, people_email=people_email, time_email=starttime)
            else:
                print("当时段无报警信息~")

            # 判断是否为定点时间，定点时间发送半天数据.
            dataFrame = []
            if str(starttime)[11:19] in str(totalTime):
                # 设置卡控，此下的定时发送一天数据的邮件和逻辑
                print('定时发邮件：')
                for o in MN:
                    dataFrame.extend(waste_history(o, endtime_oftotal, starttime))
                dataFrame = pandas.DataFrame.from_records(dataFrame,
                                                          columns=['DataTime', 'DataTypeID', 'DataValue', 'LHCodeID',
                                                                   'ValueTypeID',
                                                                   'BZlevelDown', 'BZlevelUP', 'MN', 'MNName',
                                                                   'ParamName',
                                                                   'ParamUnit', 'result'])
                print("修改报警参数上限")
                for i in range(len(dataFrame)):  # 根据项目修改上限
                    if dataFrame.iloc[i, 9] == 'CODcr':
                        dataFrame.iloc[i, 6] = 500
                    elif dataFrame.iloc[i, 9] == 'pH':
                        dataFrame.iloc[i, 6] = 9
                    elif dataFrame.iloc[i, 9] == '氨氮':
                        dataFrame.iloc[i, 6] = 45
                    elif dataFrame.iloc[i, 9] == '总铬':
                        dataFrame.iloc[i, 6] = 0.5
                    elif dataFrame.iloc[i, 9] == '总磷':
                        dataFrame.iloc[i, 6] = 8
                    elif dataFrame.iloc[i, 9] == '总镍':
                        dataFrame.iloc[i, 6] = 0.1
                    elif dataFrame.iloc[i, 9] == '总氮':
                        dataFrame.iloc[i, 6] = 70

                dataFrame_df = dataFrame[(dataFrame['BZlevelUP'] != float(0)) & (dataFrame['ValueTypeID'] == 'Avg')]
                # print(dataFrame_df)
                # print(dataFrame_df.columns)
                print('每天定时发送汇总邮件-----------------------------')
                html_df = change_style_df(dataFrame_df)
                # print(html_df)
                # print('222-----------------------------')
                sentEmailOutlook(waring_html=html_df, people_email=people_email, time_email=endtime_oftotal)

            config.set("messages", "starttime", str(last_time))
            config.set("messages", "endtime", str(last_time))
            config.write(open(path0, "r+", encoding="utf-8"))
            print('时间已更新,运行下次')
        except Exception as e:
            print(f'异常:{e}')
            config = configparser.ConfigParser()
            path0 = os.path.abspath(os.path.dirname(sys.argv[0]))  # 获取当前执行文件夹路径
            path0 = path0 + "/water.ini"
            print(path0)
            try:
                config.read(path0, encoding="utf-8-sig")  # 导出配置文件,解决文档格式为utf-8-bom问题
            except Exception as e:
                print(e)
                config.read(path0, encoding="utf-8")  # 导出配置文件
            starttime = config.get("messages", "starttime")
            print("运行数据时间为：" + starttime)
            endtime = config.get("messages", "endtime")  # 设定导出数据的时间,这个一般是当前小时的前一个小时，可修改为多个小时
            keys = config.get("messages", "keys")  # 密钥
            starttime = datetime.datetime.strptime(str(starttime), '%Y-%m-%d %H:%M:%S')
            endtime = datetime.datetime.strptime(str(endtime), '%Y-%m-%d %H:%M:%S')  # 修改时间格式,传入find_mn中
            last_time = starttime + datetime.timedelta(hours=+1)
            if starttime > datetime.datetime.now() + datetime.timedelta(hours=-1):
                break
            if last_time > datetime.datetime.now():
                last_time = datetime.datetime.now().replace(minute=0, second=0, microsecond=0) + datetime.timedelta(
                    hours=-1)
            totalTime = config.get("messages", "totalTime")
            time_end = datetime.datetime.strptime(config.get("messages", "excel_time"), '%Y-%m-%d %H:%M:%S')
            time_end = time_end_next = time_end + datetime.timedelta(days=+1)
            config.set("messages", "excel_time", str(time_end))
            config.set("messages", "starttime", str(last_time))
            config.set("messages", "endtime", str(last_time))
            config.write(open(path0, "r+", encoding="utf-8"))
