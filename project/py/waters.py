from io import StringIO
import io
import os
import sys
from email.header import Header
from email.mime.text import MIMEText
import smtplib
import simplejson
import pandas
import sqlalchemy
import datetime
import requests
from numpy import  mat as mat
from numpy import  array as array
from numpy import  unique as unique
import configparser
import win32com.client as win32
import win32com
import sqlalchemy.sql.default_comparator
import time
# 找到公司内部所有的MN，传给下个参数,配置文件的(keys)指密钥,因为经常改动,所有写在配置文件上
def find_mn(keys):
    """
    :type str
    :param keys:密钥
    :return:json
    """
    url = "http://140.179.43.204:7065/BaseService/MN_BaseExt/GetMN_BaseList?UserInfoID="+keys
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
#mn是上面find_mn出来的mn，starttime，endtime分别为时间的开始时间和结束时间，按1小时的话两者是相等的
#但如果是多个小时两者会不相同，且都为整点
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
        url1 = 'http://140.179.43.204:7065//BusinessService/MN_HisData/GetAllMN_HisData_MN?MN='
        url = url1 + str(mn) + '&' + 'StartTime=' + str(starttime) + '&' + 'EndTime=' + str(
            endtime) + '&' + 'DataTypeID=2061' + '&' + '&' + 'Rubbish=false'+'&'+'ValueTypeID=Avg'
        payload = {}
        files = {}
        headers = {
            'Authorization': 'LDKJ=5771891D20087C4A111BADC7F9FD9642',
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload, files=files)
        response = simplejson.loads(response.text)

        response_array = pandas.DataFrame.from_records(response)

        response_array = pandas.DataFrame(response_array,columns=['DataTime', 'DataTypeID', 'DataValue',
                                                                  'LHCodeID', 'ValueTypeID','BZlevelDown', 'BZlevelUP', 'MN', 'MNName', 'ParamName','ParamUnit'])
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
    except:

        response_array = pandas.DataFrame(
            columns=['DataTime', 'DataTypeID', 'DataValue', 'LHCodeID', 'ValueTypeID', 'BZlevelDown', 'BZlevelUP', 'MN',
                     'MNName', 'ParamName', 'ParamUnit'])
        response_array.loc[0, 'DataTime'] = starttime
        response_array.loc[0, 'MN'] = str(mn)
        response_array = response_array.fillna(0)
        response_array = mat(response_array)
        response_array = array(response_array)
    return response_array

#更改字体颜色的方法，给下面更改样式的使用,pandas.style.applymap会直接传参,val代表每一个datafarme里面的元素.



#更改表格样式，fort指全部导出的数据，也可以在主程序下进行筛选,输出的html_masg作为html格式,给sent_mail作参.
def change_style(fort_can):
    fort = pandas.DataFrame(fort_can, columns=[ 'ParamName', 'MNName', 'DataValue', 'BZlevelUP'])
    fort=fort.rename(columns={ 'ParamName': '项目', 'MNName': '地点', 'DataValue': '值', 'BZlevelUP': '上限'})
    fort=fort.reset_index(drop=True)
    fort.index=fort.index+1
    fort['值']=fort['值'].map(lambda x:('%.3f')%x)
    fort['上限']=fort['上限'].map(lambda x:('%.3f')%x)
    styles = [
              dict(selector="th", props=[("bgcolor", "#BEBEBE"),
                                         ("text-align", "center")]),

              ]

    fort=fort.style.\
        set_table_styles(styles).\
        applymap(change_color,subset=['值']).\
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
                      <br />
            <br />
                    <br />
            <a href="http://cnwgpm0pbi01/Reports/powerbi/WX_MetalSME/official/JGP%E5%BA%9F%E6%B0%B4%E7%9B%91%E6%B5%8B%E6%8A%A5%E5%91%8A"target="_blank">更多废水数据请查阅PowerBi</a>
            <p>自动邮件,请勿答复</p>
            </div>
    </body>
    """.format(fort=fort,starttime=str(starttime)[11:16],endtime=str(last_time)[11:16],datetime_html=str(starttime)[0:10])
    #修改html,制作email样式模板.
    html_msg = "<html>" + head + body + "</html>"


    return html_msg
    #调整发送mail时候的数据样式，确立邮件上的时间
    #转为html,发送到mail中去




def change_color(val):
    # color = 'red' if 1 < 2 else 'black'
    return 'color:red'



def change_style_df(df):
    dataFrame = pandas.DataFrame(df, columns=['DataTime', 'ParamName', 'MNName', 'DataValue', 'BZlevelUP', 'result'])
    dataFrame=dataFrame.rename(columns={'DataTime': '时间', 'ParamName': '项目', 'MNName': '地点', 'DataValue': '值', 'BZlevelUP': '上限','result':'结果'})
    dataFrame=dataFrame.reset_index(drop=True)
    dataFrame.index=dataFrame.index+1
    dataFrame=dataFrame.sort_values(by=['结果','地点'],ascending=[False,False])
    a = dataFrame[(dataFrame['结果']=='YES')].index.tolist()
    styles = [dict(selector='td', props=[('border', '1px solid black')]),
              dict(selector="th", props=[("font-size", "100%"),
                                         ('border', '1px solid black'),
                                         ("text-align", "center")]),
              dict(selector="caption", props=[("caption-side", "top")])
              ]#字典的方式给下面的style传值

    if len(a)!=0:
        dataFrame = dataFrame.style.\
            set_table_styles(styles).\
            applymap(change_color,subset=pandas.IndexSlice[float(a[0]):float(a[len(a) - 1]), ['时间', '项目', '地点', '值', '上限', '结果']]).\
            hide_columns(['结果']).\
            format({'值':'{0:,.3f}','上限':'{0:,.3f}'}).\
            hide_index().render()

    else:
        dataFrame = dataFrame.sort_values(by=['地点'], ascending=[False, False])
        dataFrame = dataFrame.style. \
            set_table_styles(styles). \
            hide_columns(['结果']). \
            hide_index().render()

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
    """.format(dataFrame=dataFrame,starttime_html=endtime_oftotal,endtime_html=starttime)
    #修改html,制作email样式模板.

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

#需要把发送人的邮箱和接收人改成配置文件上,以备后续的添加和维护
#接受的参数需为 前面输出的html，根据时间和数据预警来触发此参数
# def sent_emailoutlook(waring_html, people_email, time_email):
#     """
#
#     :param waring_html: html of datefarme
#     :param people_email:
#     :param time_email:
#     :return:
#     """
#     outlook = win32com.client.Dispatch("Outlook.Application")
#     mail_item = outlook.CreateItem(0)
#     for i in people_email:
#         mail_item.Recipients.Add(i)
#     if time_email == starttime:
#         mail_item.Subject = "JGP无锡废水超标报警"
#     elif time_email == endtime_oftotal:
#         mail_item.Subject = "JGP无锡废水定时报告"
#     elif time_email==excel_time2:
#         mail_item.Subject = "JGP无锡污废水实验室每日报告"
#     mail_item.BodyFormat = 2
#     mail_item.HTMLBody = waring_html
#     mail_item.Send()

def  sent_emailoutlook(waring_html,time_email,people_email):
    if time_email == starttime:
        mail_content = "JGP无锡废水超标报警"
    elif time_email==excel_time2:
        mail_content='JGP无锡污废水实验室报告'
    smtpServer = r'CORIMC04'
    commonPort = 587
    smtp = smtplib.SMTP("{}:{}".format(smtpServer, commonPort))
    msg = MIMEText(waring_html, "html", 'utf-8')
    msg["Subject"] = Header(mail_content, 'utf-8').encode()
    msg['from']="WasteWaterMonitor@jabil.com"
    receivers = people_email
    receivers=','.join(receivers)
    msg["To"] = receivers
    print(msg["To"])
    print(receivers)
    smtp.sendmail(to_addrs="WasteWaterMonitor@jabil.com",from_addr=receivers,msg=msg.as_string())
    smtp.quit()




def read_excel(sheetname,columns,excelname):
    """
    :param sheetname: number 指定是excel的第几页，为 1 2 3 4---max
    :param columns: datetime，指定第sheetname页的 哪一天数据
    :return: 返回datafarme,pandas里面的数据结构
    """
    excel_data=pandas.read_excel('Z:\\5.Tech Support\\1\\WATER.xlsx',sheet_name=sheetname,keep_default_na=False, engine="openpyxl",header=0)

    excel_data.columns=excel_data.columns.astype(str)

    excel_data=pandas.DataFrame(excel_data,columns=columns)
    # excel_data=excel_data.fillna('')
    if excelname==1:
        excel_data.dropna(axis=0, inplace=True)
    else:
        excel_data = excel_data.fillna('')
    return excel_data

def overweight(guest_min,guest_max,time_new,project):
    """

    :param guest_min:下限的标准列
    :param guest_max: 上限的标准列
    :param time_new: 日期列
    :param project: 地点列
    :return: 输出结果列
    """
    if guest_min<=time_new<=guest_max:
        return str('符合标准')
    elif time_new<guest_min:
        return str(project+'低于标准')
    else:
        return str(project+'超标')

def merge_group(excel_rersult):
    """

    :param excel_rersult: datafarme 的结果列
    :return: 结果列，相同地点的且不同超标结果合一
    """
    excel_rersult=list(excel_rersult)
    if '符合标准'in excel_rersult and len(excel_rersult)>1:
        excel_rersult.remove('符合标准')
        excel_rersult=','.join(excel_rersult)
        return excel_rersult
    else:
        return '符合标准'

def change_color_excel(val):
    """
    :param val:元素值，表达式datafarmede中的所有值，单个传入 对单个值的字体颜色修改
    :return: val
    """
    if '超'in val or '低'in val:
        return 'color:red'
    else:
        return 'color:black'


def change_style_excel(excel_one,time_html):
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
       <body>

        <div align="left",font-family='SimSun',"font-size"="15px">
           <p>Dear Leaders:</p>
           <p>以下为污废水实验室日常报告,请查阅！</p>
           <p>日期：{sh_sys}</p>
           <p>{dataFrame}</p>
                      <br />
            <br />
                    <br />
           <a href="http://cnwgpm0pbi01/Reports/powerbi/WX_MetalSME/official/JGP%E5%BA%9F%E6%B0%B4%E7%9B%91%E6%B5%8B%E6%8A%A5%E5%91%8A"target="_blank">更多废水数据请查阅PowerBi</a>
           <p>自动邮件,请勿答复</p>          
        </div>

    </body>
    """.format(dataFrame=excel_one,html_time=time_html,sh_sys=str(excel_time)[0:10])
    dataFrame = "<html>" + head + body + "</html>"

    return dataFrame





if __name__ == '__main__':
    # try:
        config = configparser.ConfigParser()
        config.read('C:\\water\\water.ini')  # 导出配置文件
        starttime = config.get("messages", "starttime")
        endtime = config.get("messages", "endtime")  # 设定导出数据的时间,这个一般是当前小时的前一个小时，可修改为多个小时
        keys = config.get("messages", "keys")  # 密钥
        starttime = datetime.datetime.strptime(str(starttime), '%Y-%m-%d %H:%M:%S')
        endtime = datetime.datetime.strptime(str(endtime), '%Y-%m-%d %H:%M:%S')  # 修改时间格式,传入find_mn中
        endtime_oftotal = starttime + datetime.timedelta(hours=-12)
        last_time = starttime + datetime.timedelta(hours=+1)
        totalTime = config.get("messages", "totalTime")
        # totalTime 确定需要定时发送的时间，此确定时间写在配置文件上,对比的时间为starttime.一旦相同则导出12个小时的数据.

        fort = []
        MN = list(find_mn(keys))  # 导出mn
        for o in MN:
            fort.extend(waste_history(o, starttime, endtime))  # 遍历mn,导出mn相关数据
        fort = pandas.DataFrame.from_records(fort,
                                             columns=['DataTime', 'DataTypeID', 'DataValue', 'LHCodeID', 'ValueTypeID',
                                                      'BZlevelDown', 'BZlevelUP', 'MN', 'MNName', 'ParamName',
                                                      'ParamUnit', 'result'])
        # insert_intosql(fort)#导出的全部数据插入sql
        print('数据插入成功')
        # # 后续需要判断发送半天时间，和判断发送报警邮件。
        # #如果有超标且定点发送群体邮件的的是需要改变style.color,一个小时内的超标预警不需要,可以做两个style给出.因为会有两种样式.
        # #后续会来两天发送一个的那种邮件.再做第三个style

        fort_yes = fort[(fort['result'] == 'YES') & (fort['BZlevelUP'] != float(0)) & (fort['ValueTypeID'] == 'Avg')]

        ll = list(fort_yes['result'])
        print(ll)
        people_email = config.get("messages", "people_email")
        people_email = people_email.split(",")
        # 判断是否存在超标的数据,超出的发送预警
        if 'YES' in ll:
            pandas.set_option('display.max_columns', None)
            pandas.set_option('display.max_rows', None)

            html_waring = change_style(fort_yes)

            send_waring = sent_emailoutlook(waring_html=html_waring, people_email=people_email, time_email=starttime)
            print('发送成功')

        # 判断是否为定点时间，定点时间发送半天数据.
        dataFrame = []
        if str(starttime)[11:19] in str(totalTime)and str(starttime)[11:19]==1008611 :
            print('定点时间')
            for o in MN:
                dataFrame.extend(waste_history(o, endtime_oftotal, starttime))
            dataFrame = pandas.DataFrame.from_records(dataFrame,
                                                      columns=['DataTime', 'DataTypeID', 'DataValue', 'LHCodeID',
                                                               'ValueTypeID',
                                                               'BZlevelDown', 'BZlevelUP', 'MN', 'MNName', 'ParamName',
                                                               'ParamUnit', 'result'])

            dataFrame_df = dataFrame[(dataFrame['BZlevelUP'] != float(0)) & (dataFrame['ValueTypeID'] == 'Avg')]

            html_df = change_style_df(dataFrame_df)

            send_df=sent_emailoutlook(waring_html=html_df,people_email=people_email,time_email=endtime_oftotal)

        # 确立发送实验室一日数据的触发逻辑,excel_time 配置文件的时间，也用与摄取表格中的对应列。excel_time_2,定时时间，一般为8点。
        excel_time = config.get('messages', 'excel_time')
        excel_time2 = config.get('messages', 'excel_time_2')
        html_excel_time = excel_time[0:10]
        # 写一个逻辑，配置文件的时间为一个变化时间.，且获取当前自然时间，当当前时间和现在时间都为8点，即可触发发送邮件逻辑
        time_now = time.strftime('%Y-%m-%d %H:00:00')

        time_now = str(time_now)[11:21]


        if time_now == str(excel_time2):

            data_colums = ['处理系统', '项目', '客户标准(min)', '客户标准(max)', excel_time]  # sheet1的表头
            note_colums = ['处理系统', excel_time]
            data_excel = read_excel(sheetname=0, columns=data_colums,excelname=1)
            note_excel = read_excel(sheetname=1, columns=note_colums,excelname=2)

            # 调用read_excel函数,读取excel，    #读取第二份备注的页面
            data_excel[['客户标准(min)', '客户标准(max)', excel_time, ]] = \
                data_excel[['客户标准(min)', '客户标准(max)', excel_time, ]].apply(pandas.to_numeric)
            # 修改下面字段格式，需要改成数字类型

            a=data_excel.index.tolist()

            if len(a)!=0:
                data_excel['result'] = data_excel[['客户标准(min)', '客户标准(max)', '项目', excel_time]].apply(lambda x:
                                                                                                      overweight(guest_min=x[
                                                                                                          '客户标准(min)'],
                                                                                                                 guest_max=x[
                                                                                                                     '客户标准(max)'],
                                                                                                                 time_new=x[
                                                                                                                     excel_time],
                                                                                                                 project=x[
                                                                                                                     '项目']),
                                                                                                      axis=1)
                # lambda 内置函数,调用函数overweight，输入需要的三列，得出结果列

                # data_excel_true 调取data_excel中的处理系统和结果，后面去重和修改样式，得出当天超标的数据详情,数据归一

                data_excel_true = pandas.DataFrame.from_records(data_excel, columns=['处理系统', 'result'])
                # 只保留where和结果

                data_excel_true = data_excel_true.drop_duplicates()
                data_excel_true = data_excel_true.reset_index()

                data_excel_true = data_excel_true.groupby(['处理系统'], as_index=False).apply(
                    lambda x: merge_group(excel_rersult=x['result']))
                # 对数据进行归一
                data_excel_true.columns = ['处理系统', '数据分析结果']

                note_excel_true = pandas.DataFrame.from_records(note_excel, columns=['处理系统', excel_time])
                note_excel_true.columns = ['处理系统', '实验室备注']

                excel_end = pandas.merge(data_excel_true, note_excel_true, on='处理系统')

                # excel_end 当天结果,合并后改成html格式

                people_email = config.get("messages", "people_email")
                    # split(',')

                styles = [dict(selector='td', props=[("text-align", "center")]),
                          dict(selector="th", props=[("font-size", "15px"),
                                                     ("text-align", "center")]),
                          dict(selector="caption", props=[("caption-side", "top")])
                          ]
                excel_end = excel_end.style. \
                    set_table_styles(styles).applymap(change_color_excel). \
                    hide_index().render()
                excel_end = change_style_excel(excel_one=excel_end, time_html=html_excel_time)
                sent_emailoutlook(waring_html=excel_end, people_email=people_email, time_email=excel_time2)
                time_end = datetime.datetime.strptime(config.get("messages", "excel_time"), '%Y-%m-%d %H:%M:%S')
                time_end = time_end_next = time_end + datetime.timedelta(days=+1)
                config.set("messages", "excel_time", str(time_end))
                config.write(open('c:\\water\\water.ini', "r+", encoding="utf-8"))
            else:
                time_end = datetime.datetime.strptime(config.get("messages", "excel_time"), '%Y-%m-%d %H:%M:%S')
                time_end = time_end_next = time_end + datetime.timedelta(days=+1)
                config.set("messages", "excel_time", str(time_end))
                config.write(open('c:\\water\\water.ini', "r+", encoding="utf-8"))
            # 修改配置文件的时间,给程序下一次使用
        config.set("messages", "starttime", str(last_time))
        config.set("messages", "endtime", str(last_time))
        config.write(open('c:\\water\\water.ini', "r+", encoding="utf-8"))
        print('时间已更新,运行下次')
    # except:
    #     print('异常')
    #     config = configparser.ConfigParser()
    #     config.read('C:\\water\\water.ini')  # 导出配置文件
    #     starttime = config.get("messages", "starttime")
    #     endtime = config.get("messages", "endtime")  # 设定导出数据的时间,这个一般是当前小时的前一个小时，可修改为多个小时
    #     keys = config.get("messages", "keys")  # 密钥
    #     starttime = datetime.datetime.strptime(str(starttime), '%Y-%m-%d %H:%M:%S')
    #     endtime = datetime.datetime.strptime(str(endtime), '%Y-%m-%d %H:%M:%S')  # 修改时间格式,传入find_mn中
    #
    #     last_time = starttime + datetime.timedelta(hours=+1)
    #     totalTime = config.get("messages", "totalTime")
    #     time_end = datetime.datetime.strptime(config.get("messages", "excel_time"), '%Y-%m-%d %H:%M:%S')
    #     time_end = time_end_next = time_end + datetime.timedelta(days=+1)
    #     config.set("messages", "excel_time", str(time_end))
    #
    #     config.set("messages", "starttime", str(last_time))
    #     config.set("messages", "endtime", str(last_time))
    #     config.write(open('c:\\water\\water.ini', "r+", encoding="utf-8"))







