# -*- coding: utf-8 -*- 
# @Time : 2021/4/26 17:44 
# @Author : chang
# @File : excel_1.py
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
from numpy import mat as mat
from numpy import array as array
from numpy import unique as unique
import configparser
import win32com.client as win32
import win32com
import sqlalchemy.sql.default_comparator
import time
def read_excel(sheetname, columns, excelname):
    """
    :param sheetname: number 指定是excel的第几页，为 1 2 3 4---max
    :param columns: datetime，指定第sheetname页的 哪一天数据
    :return: 返回datafarme,pandas里面的数据结构
    """
    excel_data = pandas.read_excel('Z:\\5.Tech Support\\1\\WATER.xlsx', sheet_name=sheetname, keep_default_na=False,
                                   engine="openpyxl", header=0)

    excel_data.columns = excel_data.columns.astype(str)

    excel_data = pandas.DataFrame(excel_data, columns=columns)
    # excel_data=excel_data.fillna('')
    if excelname == 1:
        excel_data.dropna(axis=0, inplace=True)
    else:
        excel_data = excel_data.fillna('')
    return excel_data
def overweight(guest_min, guest_max, time_new, project,timeresult):
    """

    :param guest_min:下限的标准列
    :param guest_max: 上限的标准列
    :param time_new: 日期列
    :param project: 地点列
    :return: 输出结果列
    """
    if guest_min <= time_new <= guest_max:
        return str('符合标准')
    elif time_new < guest_min:
        return str(project+'低于标准'+':'+'标准{a}—{b},实测{c}'.format(a=guest_min,b=guest_max,c=timeresult))
    else:
        return str(project + '超过标准'+':''标准{a}—{b},实测{c}'.format(a=guest_min,b=guest_max,c=timeresult))
def merge_group(excel_rersult):
    """

    :param excel_rersult: datafarme 的结果列
    :return: 结果列，相同地点的且不同超标结果合一
    """
    excel_rersult = list(excel_rersult)
    if '符合标准' in excel_rersult and len(excel_rersult) > 1:
        excel_rersult.remove('符合标准')
        excel_rersult = '&&&'.join(excel_rersult)
        # excel_rersult=excel_rersult+'&&&'
        return excel_rersult
    else:
        return '符合标准'
def change_color_excel(val):
    """
    :param val:元素值，表达式datafarmede中的所有值，单个传入 对单个值的字体颜色修改
    :return: val
    """
    if '超' in val or '低' in val:
        return 'color:red'
    else:
        return 'color:black'
def change_style_excel(excel_one, time_html):
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
    """.format(dataFrame=excel_one, html_time=time_html, sh_sys=str(excel_time)[0:10])
    dataFrame = "<html>" + head + body + "</html>"
    dataFrame=dataFrame.replace('&&&','<br>')
    return dataFrame



config = configparser.ConfigParser()
config.read('C:\\water\\water.ini')  # 导出配置文件
starttime = config.get("messages", "starttime")
endtime = config.get("messages", "endtime")  # 设定导出数据的时间,这个一般是当前小时的前一个小时，可修改为多个小时
keys = config.get("messages", "keys")  # 密钥
people_email = config.get("messages", "people_email")
people_email = people_email.split(",")
excel_time = config.get('messages', 'excel_time')
excel_time2 = config.get('messages', 'excel_time_2')
html_excel_time = excel_time[0:10]
# 写一个逻辑，配置文件的时间为一个变化时间.，且获取当前自然时间，当当前时间和现在时间都为8点，即可触发发送邮件逻辑
time_now = time.strftime('%Y-%m-%d %H:00:00')
time_now = str(time_now)[11:21]
data_colums = ['处理系统', '项目', '客户标准(min)', '客户标准(max)', excel_time]  # sheet1的表头
note_colums = ['处理系统', excel_time]
data_excel = read_excel(sheetname=0, columns=data_colums, excelname=1)
note_excel = read_excel(sheetname=1, columns=note_colums, excelname=2)

# 调用read_excel函数,读取excel，    #读取第二份备注的页面
data_excel[['客户标准(min)', '客户标准(max)', excel_time, ]] = \
    data_excel[['客户标准(min)', '客户标准(max)', excel_time, ]].apply(pandas.to_numeric)
# 修改下面字段格式，需要改成数字类型

a = data_excel.index.tolist()

if len(a) != 0:
    data_excel['result'] = data_excel[['客户标准(min)', '客户标准(max)', '项目', excel_time]].apply(lambda x:
                                                                                          overweight(
                                                                                              guest_min=x[
                                                                                                  '客户标准(min)'],
                                                                                              guest_max=x[
                                                                                                  '客户标准(max)'],
                                                                                              time_new=x[
                                                                                                  excel_time],
                                                                                              project=x[
                                                                                                  '项目'],timeresult=x['{a}'.format(a=excel_time)]),
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
    pandas.set_option('display.max_columns', None)
    pandas.set_option('display.max_rows', None)
    print(data_excel_true)
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
    print(excel_end)






















